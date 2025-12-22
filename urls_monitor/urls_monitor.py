#!/usr/bin/python3
import os
import traceback
import argparse
import hashlib
import difflib
import smtplib
import requests
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from bs4 import BeautifulSoup
from abc import ABC, abstractmethod
from typing import Dict, Optional

# URLs a monitorear
URLS = [
    "https://www.sodetegc.org/conocenos/informacion-administrativa/empleo/",
    "https://asistacanarias.org/portal/trabaja-con-nosotros",
    "https://www.itccanarias.org/web/es/empleo",
    "https://www3.gobiernodecanarias.org/sanidad/scs/contenidoGenerico.jsp?idDocument=0de977ff-8e1f-11f0-ab16-39979cd2dfcc&idCarpeta=b8cf85ba-fc1a-11dd-a72f-93771b0e33f6"
]

URL_TITLES = [
    "SodeteGC",
    "Asista",
    "ITCCanarias",
    "SCS: OPE 2016-17-18: General"
]


class Config:
    """Configuration constants and environment variables."""
    
    def __init__(self, config_dict: Dict[str, any]):
        """Initialize configuration from dictionary."""
        self.telegram_token = config_dict.get('telegram_token')
        self.telegram_chat_id = config_dict.get('telegram_chat_id')
        self.smtp_server = config_dict.get('smtp_server', 'smtp.gmail.com')
        self.smtp_port = config_dict.get('smtp_port', 587)
        self.email_from = config_dict.get('email_from')
        self.email_to = config_dict.get('email_to')
        self.email_password = config_dict.get('email_password')
    
    @classmethod
    def from_environment(cls) -> 'Config':
        """Load and validate environment variables."""
        telegram_token = os.getenv("TELEGRAM_BOT_TOKEN")
        telegram_chat_id = os.getenv("TELEGRAM_CHAT_ID")
        
        email_config = {
            'smtp_server': os.getenv("SMTP_SERVER", "smtp.gmail.com"),
            'smtp_port': int(os.getenv("SMTP_PORT", "587")),
            'email_from': os.getenv("EMAIL_FROM"),
            'email_to': os.getenv("EMAIL_TO"),
            'email_password': os.getenv("EMAIL_PASSWORD")
        }
        
        config_dict = {
            'telegram_token': telegram_token,
            'telegram_chat_id': telegram_chat_id,
            **email_config
        }
        
        return cls(config_dict)
    
    def has_telegram_config(self) -> bool:
        """Check if Telegram configuration is complete."""
        return bool(self.telegram_token and self.telegram_chat_id)
    
    def has_email_config(self) -> bool:
        """Check if Email configuration is complete."""
        return bool(self.email_from and self.email_password and self.email_to)


class Notifier(ABC):
    """Clase abstracta para notificadores"""
    
    @abstractmethod
    def send(self, subject: str, message: str) -> bool:
        """Envía una notificación"""
        pass


class ConsoleNotifier(Notifier):
    """Notificador que imprime en consola (stdout)"""
    
    def __init__(self, config: Optional[Config] = None):
        """Initialize console notifier (no config needed)"""
        pass
    
    def send(self, subject: str, message: str) -> bool:
        try:
            print(f"\n{'='*80}")
            print(f"NOTIFICACIÓN: {subject}")
            print(f"{'='*80}")
            print(message)
            print(f"{'='*80}\n")
            return True
        except Exception as e:
            print(f"Error en ConsoleNotifier: {e}")
            return False


class TelegramNotifier(Notifier):
    """Notificador que envía mensajes por Telegram"""
    
    def __init__(self, config: Config):
        self.config = config
        self.enabled = config.has_telegram_config()
        if not self.enabled:
            print("⚠️  TelegramNotifier no configurado (falta TELEGRAM_BOT_TOKEN o TELEGRAM_CHAT_ID)")
    
    @staticmethod
    def escape_markdown(text: str) -> str:
        """Escapa caracteres especiales para Markdown de Telegram"""
        # Caracteres que necesitan escape en Markdown
        special_chars = ['_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!']
        for char in special_chars:
            text = text.replace(char, '\\' + char)
        return text
    
    def send(self, subject: str, message: str) -> bool:
        if not self.enabled:
            return False
        
        url = f"https://api.telegram.org/bot{self.config.telegram_token}/sendMessage"
        
        # Primer intento: mensaje sin formato Markdown
        try:
            # Limpiar mensaje de caracteres problemáticos
            clean_subject = subject.replace('*', '').replace('_', '').replace('`', '')
            clean_message = message.replace('*', '').replace('_', '').replace('`', '')
            full_message = f"🔔 {clean_subject}\n\n{clean_message}"
            
            # Telegram tiene límite de 4096 caracteres
            if len(full_message) > 4096:
                full_message = full_message[:4090] + "\n..."
            
            payload = {
                "chat_id": self.config.telegram_chat_id,
                "text": full_message
            }
            
            print(f"📤 Intentando enviar mensaje a Telegram (chat_id: {self.config.telegram_chat_id})")
            print(f"📏 Longitud del mensaje: {len(full_message)} caracteres")
            
            response = requests.post(url, json=payload, timeout=10)
            
            if response.status_code == 200:
                print("✅ Mensaje enviado por Telegram")
                return True
            else:
                print(f"❌ Telegram respondió con código: {response.status_code}")
                print(f"❌ Respuesta: {response.text}")
                return False
                
        except requests.exceptions.HTTPError as e:
            print(f"❌ Error HTTP de Telegram: {e}")
            if hasattr(e.response, 'text'):
                print(f"❌ Detalle: {e.response.text}")
            return False
        except Exception as e:
            print(f"❌ Error inesperado enviando a Telegram: {e}")
            print(f"❌ Tipo de error: {type(e).__name__}")
            return False


class EmailNotifier(Notifier):
    """Notificador que envía emails"""
    
    def __init__(self, config: Config):
        self.config = config
        self.enabled = config.has_email_config()
        if not self.enabled:
            print("⚠️  EmailNotifier no configurado (falta EMAIL_FROM, EMAIL_PASSWORD o EMAIL_TO)")
    
    def send(self, subject: str, message: str) -> bool:
        if not self.enabled:
            return False
        
        try:
            msg = MIMEMultipart()
            msg['From'] = self.config.email_from
            msg['To'] = self.config.email_to
            msg['Subject'] = subject
            
            msg.attach(MIMEText(message, 'html'))
            
            with smtplib.SMTP(self.config.smtp_server, self.config.smtp_port) as server:
                server.starttls()
                server.login(self.config.email_from, self.config.email_password)
                server.send_message(msg)
            
            print("✅ Email enviado correctamente")
            return True
        except Exception as e:
            print(f"❌ Error enviando email: {e}")
            return False


def get_content_hash_and_text(url: str) -> tuple:
    """
    Descarga el contenido de la URL y devuelve su hash SHA256 y el texto extraído
    
    Returns:
        tuple: (hash_string, texto_contenido) o (None, None) si hay error
    """
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        response.encoding = response.apparent_encoding
        
        # Extraer texto limpio con BeautifulSoup
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Eliminar scripts, estilos, etc.
        for script in soup(["script", "style", "meta", "link"]):
            script.decompose()
        
        text = soup.get_text(separator='\n', strip=True)
        
        # Calcular hash del contenido
        content_hash = hashlib.sha256(text.encode('utf-8')).hexdigest()
        
        return content_hash, text
    
    except Exception as e:
        print(f"Error descargando contenido de {url}: {e}")
        return None, None


def load_previous_hash(hash_file: str) -> Optional[str]:
    """Carga el hash anterior desde el archivo"""
    if os.path.exists(hash_file):
        with open(hash_file, 'r') as f:
            return f.read().strip()
    return None


def save_current_hash(hash_file: str, current_hash: str):
    """Guarda el hash actual en el archivo"""
    with open(hash_file, 'w') as f:
        f.write(current_hash)


def log_change(log_file: str, url: str = ""):
    """Registra un cambio detectado en el archivo de log"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(log_file, 'a') as f:
        f.write(f"[{timestamp}] Cambio detectado en: {url}\n")


def calculate_diff(old_text: str, new_text: str, url_title: str) -> str:
    """
    Calcula el diff entre dos textos
    
    Returns:
        str: Diff en formato legible
    """
    old_lines = old_text.splitlines(keepends=True)
    new_lines = new_text.splitlines(keepends=True)
    
    diff = difflib.unified_diff(
        old_lines, 
        new_lines, 
        fromfile=f'{url_title}_anterior',
        tofile=f'{url_title}_actual',
        lineterm=''
    )
    
    diff_text = ''.join(diff)
    
    # Limitar el tamaño del diff para no saturar notificaciones
    max_diff_length = 2000
    if len(diff_text) > max_diff_length:
        diff_text = diff_text[:max_diff_length] + f"\n... (diff truncado, total: {len(diff_text)} caracteres)"
    
    return diff_text


def load_previous_text(data_dir: str) -> Optional[str]:
    """Carga el texto anterior más reciente"""
    try:
        files = [f for f in os.listdir(data_dir) if f.startswith("texto_descargado_") and f.endswith(".txt")]
        if not files:
            return None
        
        # Ordenar por nombre (que incluye timestamp) y tomar el más reciente
        files.sort()
        if len(files) < 2:
            return None
        
        # Cargar el penúltimo archivo (el último es el actual)
        previous_file = os.path.join(data_dir, files[-2])
        with open(previous_file, 'r', encoding='utf-8') as f:
            return f.read()
    except Exception as e:
        print(f"Error cargando texto anterior: {e}")
        return None


def notify_all(notifiers: list, subject: str, message: str):
    """Envía notificación a todos los notificadores"""
    for notifier in notifiers:
        notifier.send(subject, message)


def main(output_root_dir: str):
    # Cargar configuración desde variables de entorno
    try:
        config = Config.from_environment()
    except Exception as e:
        print(f"Error cargando configuración: {e}")
        config = Config({})  # Configuración vacía como fallback
    
    # Inicializar notificadores con configuración
    notifiers = [
        ConsoleNotifier(config),
        TelegramNotifier(config),
        EmailNotifier(config)
    ]
    
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Notificar inicio de ejecución
        start_message = f"Iniciando monitorización\n\nFecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\nMonitorizando {len(URLS)} URLs:\n\n"
        for i, (url, url_title) in enumerate(zip(URLS, URL_TITLES), 1):
            start_message += f"{i}. {url_title}\n{url}\n\n"
        
        console_notifier = ConsoleNotifier(config)
        telegram_notifier = TelegramNotifier(config)
        
        console_notifier.send("🔍 Inicio de monitorización", start_message)
        telegram_notifier.send("🔍 Inicio de monitorización", start_message)
        
        subject = 'AUTOMATIZACIÓN EP: detector de cambios url'
        body = ''
        changes_detected = False
        change_details = []
        changed_pages = []  # Lista de páginas que han cambiado
        first_time_pages = []  # Lista de páginas en primera monitorización
        error_pages = []  # Lista de páginas con errores
        unchanged_pages = 0  # Contador de páginas sin cambios
        
        for url, url_title in zip(URLS, URL_TITLES):
            body += f"----------{url_title}----------<br>"
            data_dir = os.path.join(output_root_dir, 'detector_cambios_texto_url', url_title)
            text_filename = f"texto_descargado_{timestamp}.txt"
            downloaded_text_file = os.path.join(data_dir, text_filename)
            os.makedirs(data_dir, exist_ok=True)
            
            # Archivo donde se guardará el hash del contenido anterior
            hash_file = os.path.join(data_dir, "hash_anterior.txt")
            
            # Carpeta donde se almacenará un log de cambios
            log_file = os.path.join(data_dir, "cambios_detectados.log")
            
            current_hash, text = get_content_hash_and_text(url)
            
            if current_hash:
                with open(downloaded_text_file, "w", encoding='utf-8') as f:
                    f.write(text)
            
            if not current_hash:
                body += "Error al descargar contenido<br>"
                error_pages.append(url_title)
                continue
            
            previous_hash = load_previous_hash(hash_file)
            
            if not text:
                body += "La página descargada no tiene ningún contenido, probablemente haya habido error<br>"
                error_pages.append(url_title)
            elif previous_hash is None:
                body += "Primera ejecución, guardando estado actual.<br>"
                save_current_hash(hash_file, current_hash)
                first_time_pages.append(url_title)
                
                # Notificar que es la primera vez
                first_time_message = f"ℹ️ PRIMERA MONITORIZACIÓN\n\n📄 {url_title}\n{url}\n\n⚠️ No se ha podido comparar con un punto anterior porque es la primera vez que se monitoriza esta URL.\n\nEl contenido actual se ha guardado como referencia para futuras comparaciones."
                
                console_notifier = ConsoleNotifier(config)
                telegram_notifier = TelegramNotifier(config)
                
                console_notifier.send(f"ℹ️ Primera monitorización - {url_title}", first_time_message)
                telegram_notifier.send(f"ℹ️ Primera monitorización - {url_title}", first_time_message)
            elif current_hash != previous_hash:
                body += "¡Se ha detectado un cambio en la página!<br>"
                log_change(log_file, url)
                save_current_hash(hash_file, current_hash)
                changes_detected = True
                changed_pages.append(url_title)
                
                # Calcular diff
                previous_text = load_previous_text(data_dir)
                if previous_text:
                    diff = calculate_diff(previous_text, text, url_title)
                    if diff:
                        change_details.append(f"\n📄 {url_title}\n{url}\n\nDIFF:\n{diff}\n")
                else:
                    change_details.append(f"\n📄 {url_title}\n{url}\n(No se pudo calcular diff)")
            else:
                body += "Sin cambios detectados.<br>"
                unchanged_pages += 1
            
            body += f'<a href="{url}">click aquí</a><br><br>'
        
        # Preparar resumen final
        total_urls = len(URLS)
        num_changes = len(changed_pages)
        num_first_time = len(first_time_pages)
        num_errors = len(error_pages)
        
        # Crear mensaje de resumen
        summary = f"\n{'='*60}\n"
        summary += f"📊 RESUMEN DE MONITORIZACIÓN\n"
        summary += f"{'='*60}\n\n"
        summary += f"📅 Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        summary += f"🔗 Total URLs monitorizadas: {total_urls}\n\n"
        
        if num_changes > 0:
            summary += f"🚨 CAMBIOS DETECTADOS: {num_changes} página{'s' if num_changes != 1 else ''}\n"
            for page in changed_pages:
                summary += f"   • {page}\n"
            summary += "\n"
        else:
            summary += f"✅ Sin cambios detectados\n\n"
        
        summary += f"📄 Sin cambios: {unchanged_pages} página{'s' if unchanged_pages != 1 else ''}\n"
        
        if num_first_time > 0:
            summary += f"ℹ️  Primera monitorización: {num_first_time} página{'s' if num_first_time != 1 else ''}\n"
            for page in first_time_pages:
                summary += f"   • {page}\n"
        
        if num_errors > 0:
            summary += f"❌ Errores: {num_errors} página{'s' if num_errors != 1 else ''}\n"
            for page in error_pages:
                summary += f"   • {page}\n"
        
        summary += f"\n{'='*60}\n"
        
        # Si hay cambios, notificar con el diff por todos los canales
        if changes_detected:
            notification_message = "🚨 CAMBIOS DETECTADOS EN URLS MONITORIZADAS\n\n"
            notification_message += "\n".join(change_details)
            notification_message += summary
            
            # Enviar por consola y telegram
            console_notifier = ConsoleNotifier(config)
            telegram_notifier = TelegramNotifier(config)
            
            console_notifier.send("⚠️ CAMBIOS DETECTADOS - Empleo Público", notification_message)
            telegram_notifier.send("⚠️ CAMBIOS DETECTADOS - Empleo Público", notification_message)
        else:
            # Si no hay cambios, enviar solo el resumen
            console_notifier = ConsoleNotifier(config)
            telegram_notifier = TelegramNotifier(config)
            
            console_notifier.send("✅ Monitorización completada - Sin cambios", summary)
            telegram_notifier.send("✅ Monitorización completada - Sin cambios", summary)
        
        # Añadir resumen al body del email (en formato HTML)
        body += "<br><br>"
        body += f"<div style='background-color: #f0f0f0; padding: 15px; border-radius: 5px; font-family: monospace;'>"
        body += f"<h3>📊 RESUMEN DE MONITORIZACIÓN</h3>"
        body += f"<p><strong>📅 Fecha:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>"
        body += f"<p><strong>🔗 Total URLs monitorizadas:</strong> {total_urls}</p>"
        
        if num_changes > 0:
            body += f"<p style='color: red;'><strong>🚨 CAMBIOS DETECTADOS:</strong> {num_changes} página{'s' if num_changes != 1 else ''}</p>"
            body += "<ul>"
            for page in changed_pages:
                body += f"<li>{page}</li>"
            body += "</ul>"
        else:
            body += f"<p style='color: green;'><strong>✅ Sin cambios detectados</strong></p>"
        
        body += f"<p><strong>📄 Sin cambios:</strong> {unchanged_pages} página{'s' if unchanged_pages != 1 else ''}</p>"
        
        if num_first_time > 0:
            body += f"<p><strong>ℹ️ Primera monitorización:</strong> {num_first_time} página{'s' if num_first_time != 1 else ''}</p>"
            body += "<ul>"
            for page in first_time_pages:
                body += f"<li>{page}</li>"
            body += "</ul>"
        
        if num_errors > 0:
            body += f"<p style='color: orange;'><strong>❌ Errores:</strong> {num_errors} página{'s' if num_errors != 1 else ''}</p>"
            body += "<ul>"
            for page in error_pages:
                body += f"<li>{page}</li>"
            body += "</ul>"
        
        body += "</div>"
        
        # Enviar siempre el resumen por email (con o sin cambios)
        email_notifier = EmailNotifier(config)
        email_notifier.send(subject, body)
        
    except Exception:
        error_message = f'Error ejecutando el script:\n{traceback.format_exc()}'
        notify_all(
            notifiers,
            'AUTOMATIZACIÓN EP: Error detector de cambios url',
            error_message
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Detector de cambios en URLs.")
    parser.add_argument(
        "--output-dir",
        required=True,
        help="Directorio raíz donde se creará 'detector_cambios_texto_url/'."
    )
    args = parser.parse_args()
    main(args.output_dir)
