#!/usr/bin/env python3
"""
Multi-Source Public Employment Scraper
Monitors public employment announcements from multiple sources (BOC, GobCan, BOP Las Palmas, BOP Santa Cruz)
BOC runs Monday-Friday, Tablón runs every day, BOP sources run on Sundays only
"""

import os
import sys
import time
import traceback
import re
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import List, Optional, Tuple, Dict

import requests
from bs4 import BeautifulSoup
import google.generativeai as genai


class Config:
    """Configuration constants and environment variables."""
    
    # General settings
    REQUEST_TIMEOUT = 10
    REQUEST_DELAY = 1
    
    # HTTP Headers
    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/114.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Referer": "https://google.com",
    }
    
    # Target professions
    TARGET_PROFESSIONS = [
        'ingeniero de telecomunicación',
        'ingeniero informático',
        'ingeniero en tecnologías de la información',
        'programador',
        'médico geriatra',
        'experto o ingeniero TIC',
        'ingeniero electrónico'
    ]
    
    # Gemini model
    GEMINI_MODEL = "gemini-2.5-flash-lite"
    
    @classmethod
    def from_environment(cls) -> Dict[str, str]:
        """Load and validate environment variables."""
        telegram_token = os.getenv("TELEGRAM_BOT_TOKEN")
        telegram_chat = os.getenv("TELEGRAM_CHAT_ID")
        ai_key = os.getenv("AI_API_KEY")
        email_config = {
            'smtp_server': os.getenv("SMTP_SERVER"),
            'smtp_port': os.getenv("SMTP_PORT"),
            'email_from': os.getenv("EMAIL_FROM"),
            'email_to': os.getenv("EMAIL_TO"),
            'email_password': os.getenv("EMAIL_PASSWORD")
        }
        
        if not all([telegram_token, telegram_chat, ai_key]):
            raise ValueError(
                "Missing required environment variables: "
                "TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, AI_API_KEY"
            )
        
        return {
            'telegram_token': telegram_token,
            'telegram_chat': telegram_chat,
            'ai_key': ai_key,
            'email': email_config
        }


class WebScraper:
    """Handles web scraping operations."""
    
    @staticmethod
    def fetch_page_text(url: str, verify: bool = True) -> str:
        """
        Download and extract text content from a web page.
        
        Args:
            url: The URL to fetch
            verify: Whether to verify SSL certificates
            
        Returns:
            Extracted text content
        """
        try:
            print(f"[+] Fetching: {url}")
            response = requests.get(
                url,
                timeout=Config.REQUEST_TIMEOUT,
                verify=verify,
                headers=Config.HEADERS
            )
            
            if response.status_code == 404:
                raise Exception(f"[!] Page not found (404): {url}")
            
            response.encoding = response.apparent_encoding or 'utf-8'
            soup = BeautifulSoup(response.text, "html.parser")
            
            # Remove script and style tags
            for tag in soup(["script", "style"]):
                tag.decompose()
            
            # Extract and clean text
            text = soup.get_text(separator="\n", strip=True)
            text = '\n'.join(
                line.strip() 
                for line in text.splitlines() 
                if line.strip()
            )
            
            return text
            
        except Exception as e:
            print(f"[!] Error processing {url}: {e}")
            return ""
    
    @staticmethod
    def fetch_page_html(url: str, verify: bool = True) -> str:
        """
        Download HTML content from a web page.
        
        Args:
            url: The URL to fetch
            verify: Whether to verify SSL certificates
            
        Returns:
            HTML content
        """
        try:
            print(f"[+] Fetching HTML: {url}")
            response = requests.get(
                url,
                timeout=Config.REQUEST_TIMEOUT,
                verify=verify,
                headers=Config.HEADERS
            )
            response.encoding = response.apparent_encoding or 'utf-8'
            return response.text
        except Exception as e:
            print(f"[!] Error fetching HTML from {url}: {e}")
            return ""


class DataSource(ABC):
    """Abstract base class for data sources."""
    
    def __init__(self, name: str):
        """
        Initialize data source.
        
        Args:
            name: Name of the data source
        """
        self.name = name
        self.scraper = WebScraper()
    
    @abstractmethod
    def get_urls_to_scrape(self) -> List[str]:
        """
        Get list of URLs to scrape.
        
        Returns:
            List of URLs
        """
        pass
    
    @abstractmethod
    def extract_content(self, urls: List[str]) -> str:
        """
        Extract content from URLs.
        
        Args:
            urls: List of URLs to process
            
        Returns:
            Combined text content
        """
        pass
    
    def get_metadata(self) -> Dict[str, str]:
        """
        Get metadata about the scraped content.
        
        Returns:
            Dictionary with metadata (URLs, dates, etc.)
        """
        return {"source": self.name}
    
    def should_run_today(self) -> bool:
        """
        Determine if this source should run today.
        Override in subclasses to implement custom schedules.
        
        Returns:
            True if should run, False otherwise
        """
        return True


class BOCDataSource(DataSource):
    """BOC (Boletín Oficial de Canarias) data source."""
    
    BOC_BASE_URL = "https://www.gobiernodecanarias.org/boc/{year}/{page:03d}/index.html"
    MAX_PAGE_NUMBER = 366
    
    def __init__(self, year: Optional[int] = None):
        """
        Initialize BOC data source.
        
        Args:
            year: Year to monitor (defaults to current year)
        """
        super().__init__("BOC")
        self.year = year or datetime.now().year
    
    def should_run_today(self) -> bool:
        """
        BOC only runs Monday to Friday (weekday 0-4).
        
        Returns:
            True if today is Monday-Friday, False if weekend
        """
        weekday = datetime.now().weekday()  # 0=Monday, 6=Sunday
        should_run = weekday < 5  # 0-4 = Monday to Friday
        
        if not should_run:
            print(f"[i] {self.name} skipped: Only runs Monday-Friday (today is {['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'][weekday]})")
        
        return should_run
    
    def get_url(self, page_number: int) -> str:
        """Generate URL for a specific page number."""
        return self.BOC_BASE_URL.format(year=self.year, page=page_number)
    
    def check_page_exists(self, url: str) -> Optional[str]:
        """Check if a page exists and return its content."""
        try:
            print(f"[+] Checking: {url}")
            response = requests.get(
                url,
                timeout=5,
                headers=Config.HEADERS,
                allow_redirects=True,
                verify=True
            )
            response.encoding = response.apparent_encoding
            
            if response.status_code == 200 and "Page Not Found" not in response.text:
                return response.text
                
        except requests.RequestException as e:
            print(f"[!] Error checking {url}: {e}")
        
        return None
    
    def find_latest_page(self) -> Tuple[Optional[int], Optional[str]]:
        """Find the latest existing page using binary search."""
        low, high = 1, self.MAX_PAGE_NUMBER
        latest_page = None
        latest_content = None
        
        while low <= high:
            mid = (low + high) // 2
            url = self.get_url(mid)
            content = self.check_page_exists(url)
            
            if content:
                latest_page = mid
                latest_content = content
                low = mid + 1
            else:
                high = mid - 1
            
            time.sleep(Config.REQUEST_DELAY)
        
        return latest_page, latest_content
    
    def get_urls_to_scrape(self) -> List[str]:
        """Get the latest BOC page URL."""
        print(f"[+] Searching for latest BOC page for year {self.year}...")
        page_number, _ = self.find_latest_page()
        
        if page_number is None:
            raise Exception(f"Could not find latest BOC page for {self.year}")
        
        return [self.get_url(page_number)]
    
    def extract_content(self, urls: List[str]) -> str:
        """Extract text content from BOC URLs."""
        texts = []
        for url in urls:
            text = self.scraper.fetch_page_text(url)
            if text:
                texts.append(text)
            time.sleep(Config.REQUEST_DELAY)
        
        return '\n\n'.join(texts)
    
    def get_metadata(self) -> Dict[str, str]:
        """Get metadata about BOC content."""
        return {
            "source": self.name,
            "year": str(self.year),
            "date": datetime.now().strftime("%Y-%m-%d"),
            "weekday": datetime.now().strftime("%A")
        }


class TablonGobCanDataSource(DataSource):
    """Tablón de Anuncios GobCan data source."""
    
    BASE_URL = "https://sede.gobiernodecanarias.org/sede/movil/menu_portada_movil/tablon_anuncios"
    
    def __init__(self, days_back: int = 0):
        """
        Initialize Tablón GobCan data source.
        
        Args:
            days_back: Number of days to look back (0 = today only)
        """
        super().__init__("Tablón GobCan")
        self.days_back = days_back
        self.fecha_inicio, self.fecha_fin = self._get_date_range()
    
    def should_run_today(self) -> bool:
        """
        Tablón runs every day.
        
        Returns:
            Always True
        """
        return True
    
    def _get_date_range(self) -> Tuple[str, str]:
        """Get date range for search."""
        hoy = datetime.today()
        fecha_fin = hoy.strftime("%Y-%m-%d")
        fecha_inicio = (hoy - timedelta(days=self.days_back)).strftime("%Y-%m-%d")
        return fecha_inicio, fecha_fin
    
    def _get_num_pages(self, html: str) -> int:
        """Extract number of pages from HTML."""
        match = re.search(r"pages:(\d+)", html)
        return int(match.group(1)) if match else 1
    
    def get_urls_to_scrape(self) -> List[str]:
        """Get all Tablón page URLs for the date range."""
        print(f"[+] Searching Tablón between {self.fecha_inicio} and {self.fecha_fin}")
        
        # Fetch first page to get total number of pages
        first_url = (
            f"{self.BASE_URL}?qa&inicio=false&fh={self.fecha_fin}"
            f"&fAgrupacionMateria=true&fd={self.fecha_inicio}&page=1"
        )
        print(f"[+] First URL: {first_url}")
        
        html = self.scraper.fetch_page_html(first_url)
        if not html:
            raise Exception("Could not fetch first page from Tablón")
        
        num_pages = self._get_num_pages(html)
        print(f"[+] Number of pages: {num_pages}")
        
        # Generate all page URLs
        urls = []
        for page in range(1, num_pages + 1):
            url = (
                f"{self.BASE_URL}?qa&inicio=false&fh={self.fecha_fin}"
                f"&fAgrupacionMateria=true&fd={self.fecha_inicio}&page={page}"
            )
            urls.append(url)
        
        return urls
    
    def extract_content(self, urls: List[str]) -> str:
        """Extract text content from Tablón URLs."""
        texts = []
        for url in urls:
            text = self.scraper.fetch_page_text(url)
            if text:
                texts.append(text)
            time.sleep(Config.REQUEST_DELAY)
        
        return '\n\n'.join(texts)
    
    def get_metadata(self) -> Dict[str, str]:
        """Get metadata about Tablón content."""
        return {
            "source": self.name,
            "date_start": self.fecha_inicio,
            "date_end": self.fecha_fin,
            "days_back": str(self.days_back),
            "weekday": datetime.now().strftime("%A")
        }


class BOPDataSource(DataSource):
    """Base class for BOP (Boletín Oficial Provincial) data sources."""
    
    def __init__(self, name: str, base_url: str, verify_ssl: bool = True):
        """
        Initialize BOP data source.
        
        Args:
            name: Name of the BOP source
            base_url: Base URL for the BOP
            verify_ssl: Whether to verify SSL certificates
        """
        super().__init__(name)
        self.base_url = base_url
        self.verify_ssl = verify_ssl
    
    def should_run_today(self) -> bool:
        """
        BOP sources only run on Sundays (weekday 6).
        
        Returns:
            True if today is Sunday, False otherwise
        """
        weekday = datetime.now().weekday()  # 0=Monday, 6=Sunday
        should_run = weekday == 6  # Only Sunday
        
        if not should_run:
            weekday_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
            print(f"[i] {self.name} skipped: Only runs on Sundays (today is {weekday_names[weekday]})")
        
        return should_run
    
    def _get_monday_wednesday_friday_dates(self) -> List[str]:
        """
        Get dates for Monday, Wednesday, and Friday of the current week.
        
        Returns:
            List of dates in YYYY-MM-DD format
        """
        hoy = datetime.today()
        # Calculate start of week (Monday)
        inicio_semana = hoy - timedelta(days=hoy.weekday())
        # Days we want: Monday (0), Wednesday (2), Friday (4)
        dias_objetivo = [0, 2, 4]
        # Generate dates
        fechas = [(inicio_semana + timedelta(days=d)).strftime('%Y-%m-%d') for d in dias_objetivo]
        return fechas
    
    def get_urls_to_scrape(self) -> List[str]:
        """
        Get all BOP URLs for the current week (Monday, Wednesday, Friday).
        
        Returns:
            List of URLs to scrape
        """
        fechas = self._get_monday_wednesday_friday_dates()
        urls = []
        
        for fecha in fechas:
            print(f"[+] Adding URLs for date: {fecha}")
            # Boletín
            urls.append(f"{self.base_url}/sumario.php?fecha_mas_reciente={fecha}")
            # Anexo
            urls.append(f"{self.base_url}/sumario1.php?fecha_mas_reciente={fecha}")
            # Extraordinario
            urls.append(f"{self.base_url}/sumario2.php?fecha_mas_reciente={fecha}")
        
        return urls
    
    def extract_content(self, urls: List[str]) -> str:
        """
        Extract text content from BOP URLs.
        
        Args:
            urls: List of URLs to process
            
        Returns:
            Combined text content
        """
        texts = []
        for url in urls:
            text = self.scraper.fetch_page_text(url, verify=self.verify_ssl)
            if text:
                texts.append(text)
            time.sleep(Config.REQUEST_DELAY)
        
        return '\n\n'.join(texts)
    
    def get_metadata(self) -> Dict[str, str]:
        """Get metadata about BOP content."""
        fechas = self._get_monday_wednesday_friday_dates()
        return {
            "source": self.name,
            "dates_scraped": ', '.join(fechas),
            "weekday": datetime.now().strftime("%A"),
            "week_dates": f"{fechas[0]} to {fechas[-1]}"
        }


class BOPLasPalmasDataSource(BOPDataSource):
    """BOP Las Palmas data source."""
    
    def __init__(self):
        """Initialize BOP Las Palmas data source."""
        super().__init__(
            name="BOP Las Palmas",
            base_url="https://www.boplaspalmas.net/nbop2",
            verify_ssl=True
        )


class BOPSantaCruzDataSource(BOPDataSource):
    """BOP Santa Cruz de Tenerife data source."""
    
    def __init__(self):
        """Initialize BOP Santa Cruz data source."""
        super().__init__(
            name="BOP Santa Cruz",
            base_url="https://www.bopsantacruzdetenerife.es/bopsc2",
            verify_ssl=False  # SSL verification disabled as per original script
        )


class GeminiAnalyzer:
    """Handles AI analysis using Google Gemini."""
    
    def __init__(self, api_key: str):
        """Initialize Gemini analyzer."""
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel(Config.GEMINI_MODEL)
    
    def analyze_job_announcements(self, text: str, source_name: str = "") -> str:
        """Analyze text for public job announcements."""
        professions_list = ', '.join(Config.TARGET_PROFESSIONS)
        source_info = f" de {source_name}" if source_name else ""
        
        prompt = f"""
Analiza el contenido{source_info} y determina si se menciona alguna **convocatoria de empleo público** 
o procesos relacionados con la contratación o selección de personal. Presta atención a referencias como:

- Convocatorias de oposiciones
- Concursos de méritos
- Creación de listas de reserva o bolsas de trabajo
- Listados de admitidos o excluidos
- Resoluciones de nombramientos
- Cualquier trámite vinculado a empleo público

Indica si alguna de estas categorías afecta a las siguientes profesiones:

{professions_list}

**Instrucciones para el formato de respuesta:**
- Usa un estilo claro y fácil de leer en un chat
- Evita HTML, Markdown o formatos de código
- Resalta la información relevante con símbolos o emojis
- Cada registro debe contener, en este orden:
    1. Profesión o categoría afectada (resaltar si coincide con las profesiones indicadas)
    2. Lugar (isla, localidad, ayuntamiento o empresa, si se menciona)
    3. Tipo de información detectada (convocatoria, bases, listado de admitidos, nombramientos, etc.)

**Texto a analizar:**
{text}
"""
        
        response = self.model.generate_content(prompt)
        return response.text


class Notifier(ABC):
    """Base notifier interface."""
    
    @abstractmethod
    def send_message(self, subject: str, message: str, metadata: Optional[Dict] = None) -> None:
        """Send a notification message."""
        pass


class ConsoleNotifier(Notifier):
    """Handles console output notifications."""
    
    def send_message(self, subject: str, message: str, metadata: Optional[Dict] = None) -> None:
        """Print a message to the console."""
        print("\n" + "="*80)
        print(f"📢 {subject}")
        print("="*80)
        if metadata:
            print("Metadata:", metadata)
            print("-"*80)
        print(message)
        print("="*80 + "\n")


class TelegramNotifier(Notifier):
    """Handles Telegram notifications."""
    
    def __init__(self, bot_token: str, chat_id: str):
        """Initialize Telegram notifier."""
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.api_url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    
    def _escape_markdown(self, text: str) -> str:
        """
        Escape special Markdown characters for Telegram.
        
        Args:
            text: Text to escape
            
        Returns:
            Escaped text
        """
        # Characters that need escaping in Telegram Markdown
        special_chars = ['_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!']
        
        for char in special_chars:
            text = text.replace(char, '\\' + char)
        
        return text
    
    def _split_message(self, text: str, max_length: int = 4000) -> List[str]:
        """
        Split a message into chunks that fit Telegram's limit.
        
        Args:
            text: Text to split
            max_length: Maximum length per chunk
            
        Returns:
            List of message chunks
        """
        if len(text) <= max_length:
            return [text]
        
        chunks = []
        current_chunk = ""
        
        # Split by lines to avoid breaking in the middle of a line
        lines = text.split('\n')
        
        for line in lines:
            # If a single line is longer than max_length, split it by words
            if len(line) > max_length:
                words = line.split(' ')
                for word in words:
                    if len(current_chunk) + len(word) + 1 <= max_length:
                        current_chunk += word + ' '
                    else:
                        if current_chunk:
                            chunks.append(current_chunk.strip())
                        current_chunk = word + ' '
            # If adding this line exceeds max_length, save current chunk
            elif len(current_chunk) + len(line) + 1 > max_length:
                if current_chunk:
                    chunks.append(current_chunk.strip())
                current_chunk = line + '\n'
            else:
                current_chunk += line + '\n'
        
        # Add remaining chunk
        if current_chunk:
            chunks.append(current_chunk.strip())
        
        return chunks
    
    def send_message(self, subject: str, message: str, metadata: Optional[Dict] = None) -> None:
        """Send a message to Telegram, splitting into multiple messages if needed."""
        try:
            # Prepare full message (don't use Markdown formatting)
            full_message = f"🔔 {subject}\n\n{message}"
            if metadata:
                full_message += f"\n\n📋 Metadata: {str(metadata)}"
            
            # Split message into chunks
            max_length = 4000  # Leave margin for Telegram's 4096 char limit
            chunks = self._split_message(full_message, max_length)
            
            print(f"[+] Sending {len(chunks)} Telegram message(s)...")
            
            # Send each chunk
            for i, chunk in enumerate(chunks, 1):
                # Add part indicator if message was split
                if len(chunks) > 1:
                    chunk_header = f"📨 Parte {i}/{len(chunks)}\n\n"
                    chunk = chunk_header + chunk
                
                try:
                    response = requests.post(
                        self.api_url,
                        json={
                            "chat_id": self.chat_id,
                            "text": chunk
                        },
                        timeout=10
                    )
                    
                    if response.status_code != 200:
                        print(f"[!] Telegram API error: {response.status_code} - {response.text}")
                    else:
                        print(f"[+] Telegram chunk {i}/{len(chunks)} sent successfully")
                    
                    # Small delay between messages to avoid rate limiting
                    if i < len(chunks):
                        time.sleep(0.5)
                        
                except Exception as chunk_error:
                    print(f"[!] Error sending chunk {i}/{len(chunks)}: {chunk_error}")
                    # Continue with next chunk even if one fails
                    continue
            
            print(f"[+] All Telegram message(s) sent")
                
        except Exception as e:
            print(f"[!] Error sending Telegram message: {e}")
            traceback.print_exc()


class EmailNotifier(Notifier):
    """Handles email notifications."""
    
    def __init__(self, config: Dict[str, str]):
        """Initialize Email notifier."""
        self.smtp_server = config.get('smtp_server')
        self.smtp_port = config.get('smtp_port')
        self.email_from = config.get('email_from')
        self.email_to = config.get('email_to')
        self.email_password = config.get('email_password')
        
        # Check if email is configured
        self.is_configured = all([
            self.smtp_server, self.smtp_port, 
            self.email_from, self.email_to, self.email_password
        ])
    
    def send_message(self, subject: str, message: str, metadata: Optional[Dict] = None) -> None:
        """Send an email notification."""
        if not self.is_configured:
            print("[!] Email notifier not configured, skipping...")
            return
        
        try:
            import smtplib
            from email.mime.text import MIMEText
            from email.mime.multipart import MIMEMultipart
            
            msg = MIMEMultipart('alternative')
            msg['Subject'] = subject
            msg['From'] = self.email_from
            msg['To'] = self.email_to
            
            # Create HTML body
            html_body = f"<html><body>{message.replace(chr(10), '<br>')}</body></html>"
            if metadata:
                html_body += f"<hr><p><small>Metadata: {metadata}</small></p>"
            
            msg.attach(MIMEText(html_body, 'html'))
            
            with smtplib.SMTP(self.smtp_server, int(self.smtp_port)) as server:
                server.starttls()
                server.login(self.email_from, self.email_password)
                server.send_message(msg)
                
            print("[+] Email sent successfully")
            
        except Exception as e:
            print(f"[!] Error sending email: {e}")


class NotificationManager:
    """Manages multiple notification channels."""
    
    def __init__(self):
        """Initialize notification manager."""
        self.notifiers = []
    
    def add_notifier(self, notifier: Notifier) -> None:
        """Add a notifier to the manager."""
        self.notifiers.append(notifier)
    
    def send_message(self, subject: str, message: str, metadata: Optional[Dict] = None) -> None:
        """Send a message through all registered notifiers."""
        for notifier in self.notifiers:
            try:
                notifier.send_message(subject, message, metadata)
            except Exception as e:
                print(f"[!] Error in notifier {type(notifier).__name__}: {e}")


class MultiSourceMonitor:
    """Monitors multiple data sources for job announcements."""
    
    def __init__(self, analyzer: GeminiAnalyzer, notification_manager: NotificationManager):
        """
        Initialize multi-source monitor.
        
        Args:
            analyzer: GeminiAnalyzer instance
            notification_manager: NotificationManager instance
        """
        self.analyzer = analyzer
        self.notification_manager = notification_manager
        self.sources = []
    
    def add_source(self, source: DataSource) -> None:
        """Add a data source to monitor."""
        self.sources.append(source)
    
    def process_source(self, source: DataSource) -> None:
        """Process a single data source."""
        try:
            print(f"\n{'='*80}")
            print(f"[+] Processing source: {source.name}")
            print(f"{'='*80}\n")
            
            # Check if source should run today
            if not source.should_run_today():
                print(f"[i] Skipping {source.name} (not scheduled for today)")
                return
            
            # Get URLs to scrape
            urls = source.get_urls_to_scrape()
            print(f"[+] Found {len(urls)} URL(s) to process")
            
            # Extract content
            content = source.extract_content(urls)
            if not content:
                raise Exception(f"No content extracted from {source.name}")
            
            print(f"[+] Extracted {len(content)} characters")
            
            # Analyze with AI
            print(f"[+] Analyzing content with Gemini...")
            analysis = self.analyzer.analyze_job_announcements(content, source.name)
            
            # Get metadata
            metadata = source.get_metadata()
            metadata['urls'] = urls[:3]  # Include first 3 URLs
            
            # Send notification
            subject = f"AUTOMATIZACIÓN EP: {source.name}"
            self.notification_manager.send_message(subject, analysis, metadata)
            
            print(f"[+] Successfully processed {source.name}")
            
        except Exception as e:
            error_msg = f"Error processing {source.name}: {str(e)}\n\n{traceback.format_exc()}"
            print(f"[!] {error_msg}")
            
            try:
                subject = f"AUTOMATIZACIÓN EP: Error {source.name}"
                self.notification_manager.send_message(subject, error_msg)
            except Exception as notification_error:
                print(f"[!] Failed to send error notification: {notification_error}")
    
    def run(self) -> None:
        """Process all configured sources."""
        now = datetime.now()
        weekday_name = now.strftime("%A")
        
        print(f"\n{'#'*80}")
        print(f"# MULTI-SOURCE PUBLIC EMPLOYMENT MONITOR")
        print(f"# Starting at: {now.strftime('%Y-%m-%d %H:%M:%S')} ({weekday_name})")
        print(f"# Sources configured: {len(self.sources)}")
        print(f"{'#'*80}\n")
        
        sources_processed = 0
        sources_skipped = 0
        
        for source in self.sources:
            if source.should_run_today():
                self.process_source(source)
                sources_processed += 1
            else:
                sources_skipped += 1
            time.sleep(Config.REQUEST_DELAY)
        
        print(f"\n{'#'*80}")
        print(f"# COMPLETED ALL SOURCES")
        print(f"# Processed: {sources_processed}, Skipped: {sources_skipped}")
        print(f"# Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'#'*80}\n")


def main():
    """Main execution function."""
    notification_manager = None
    
    try:
        # Load configuration
        config = Config.from_environment()
        
        # Initialize notification manager
        notification_manager = NotificationManager()
        notification_manager.add_notifier(ConsoleNotifier())
        notification_manager.add_notifier(
            TelegramNotifier(config['telegram_token'], config['telegram_chat'])
        )
        notification_manager.add_notifier(EmailNotifier(config['email']))
        
        # Initialize analyzer
        analyzer = GeminiAnalyzer(config['ai_key'])
        
        # Initialize monitor
        monitor = MultiSourceMonitor(analyzer, notification_manager)
        
        # Add data sources
        # BOC will only run Monday-Friday (handled by should_run_today())
        monitor.add_source(BOCDataSource())
        
        # Tablón will run every day (handled by should_run_today())
        monitor.add_source(TablonGobCanDataSource(days_back=0))

        # BOP of LP and SCT will run every Sunday (handled by should_run_today())
        monitor.add_source(BOPLasPalmasDataSource())
        monitor.add_source(BOPSantaCruzDataSource())
        
        # Run monitoring
        monitor.run()
        
        print("[+] All processes completed successfully!")
        
    except Exception as e:
        error_trace = traceback.format_exc()
        error_msg = f"CRITICAL ERROR\n\n{error_trace}"
        print(f"[!] {error_msg}")
        
        # Try to send notification even if main setup failed
        try:
            if notification_manager is None:
                # Try to create a minimal notification manager
                config = Config.from_environment()
                notification_manager = NotificationManager()
                notification_manager.add_notifier(ConsoleNotifier())
                notification_manager.add_notifier(
                    TelegramNotifier(config['telegram_token'], config['telegram_chat'])
                )
            
            notification_manager.send_message(
                "AUTOMATIZACIÓN EP: CRITICAL ERROR",
                error_msg
            )
            print("[+] Error notification sent")
        except Exception as notification_error:
            print(f"[!] Failed to send critical error notification: {notification_error}")
            traceback.print_exc()
        
        sys.exit(1)


if __name__ == "__main__":
    main()
