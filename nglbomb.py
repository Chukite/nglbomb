from __future__ import annotations

import argparse
import concurrent.futures
import csv
import http.client
import json
import os
import random
import re
import signal
import socket
import string
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum, auto
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Callable, Tuple, Deque
from collections import deque
from contextlib import contextmanager

# Rich imports for premium UI
try:
    from rich.console import Console, Group
    from rich.progress import (
        Progress, SpinnerColumn, TextColumn, BarColumn, 
        TaskProgressColumn, TimeRemainingColumn, MofNCompleteColumn,
        TimeElapsedColumn
    )
    from rich.table import Table
    from rich.panel import Panel
    from rich.live import Live
    from rich.layout import Layout
    from rich.text import Text
    from rich import box
    from rich.align import Align
    from rich.columns import Columns
    from rich.tree import Tree
    from rich.syntax import Syntax
    from rich.markdown import Markdown
    from rich.prompt import Prompt, Confirm, IntPrompt, FloatPrompt
    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False

# -------------------- Configuration --------------------
DEFAULT_ENDPOINT = "https://ngl.link/api/submit"
MIN_JITTER_SECONDS = 0.05
MAX_BACKOFF_SECONDS = 60.0
DEFAULT_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
CONFIG_DIR = Path.home() / ".nglbomb"
CONFIG_FILE = CONFIG_DIR / "config.json"

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Android 14; Mobile; rv:121.0) Gecko/121.0 Firefox/121.0",
]

SPINNERS = ["dots", "line", "arrow", "clock", "moon", "runner", "bouncingBar"]


class MessageStrategy(Enum):
    SEQUENTIAL = "sequential"
    RANDOM = "random"
    RANDOM_WEIGHTED = "random_weighted"
    ROUND_ROBIN = "round_robin"


@dataclass(frozen=True)
class Config:
    username: str
    count: int
    workers: int
    delay: float
    timeout: float
    retries: int
    endpoint: str
    questions: List[str]
    message_strategy: MessageStrategy = MessageStrategy.SEQUENTIAL
    use_proxy: bool = False
    proxy_list: List[str] = field(default_factory=list)
    custom_headers: Dict[str, str] = field(default_factory=dict)
    export_format: Optional[str] = None
    export_path: Optional[str] = None
    resume: bool = False
    dry_run: bool = False
    verbose: bool = False
    adaptive_delay: bool = True
    max_delay: float = 30.0
    min_delay: float = 0.1
    rotate_ua: bool = True
    connection_pool_size: int = 10
    show_dashboard: bool = True
    max_history: int = 100


@dataclass
class Result:
    index: int
    ok: bool
    status: int | None
    error: str | None
    duration_ms: float = 0.0
    attempt: int = 1
    timestamp: str = ""
    proxy_used: Optional[str] = None
    user_agent: str = ""


@dataclass
class Stats:
    total: int = 0
    success: int = 0
    failed: int = 0
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    avg_duration_ms: float = 0.0
    min_duration_ms: float = float('inf')
    max_duration_ms: float = 0.0
    errors_by_type: Dict[str, int] = field(default_factory=dict)
    total_bytes_sent: int = 0
    total_bytes_received: int = 0
    # Real-time metrics
    current_rps: float = 0.0
    last_update_time: float = 0.0
    success_history: Deque[bool] = field(default_factory=lambda: deque(maxlen=100))
    duration_history: Deque[float] = field(default_factory=lambda: deque(maxlen=100))

    @property
    def elapsed_seconds(self) -> float:
        if self.start_time is None:
            return 0.0
        end = self.end_time or time.time()
        return end - self.start_time

    @property
    def rate_per_second(self) -> float:
        elapsed = self.elapsed_seconds
        if elapsed == 0:
            return 0.0
        return self.success / elapsed

    @property
    def success_rate(self) -> float:
        if self.total == 0:
            return 0.0
        return (self.success / self.total) * 100

    @property
    def recent_success_rate(self) -> float:
        if not self.success_history:
            return 0.0
        return (sum(self.success_history) / len(self.success_history)) * 100

    @property
    def avg_recent_duration(self) -> float:
        if not self.duration_history:
            return 0.0
        return sum(self.duration_history) / len(self.duration_history)


# -------------------- Global State --------------------
_shutdown_event = threading.Event()
_stats = Stats()
_stats_lock = threading.Lock()
_current_delay = 0.0
_delay_lock = threading.Lock()
_results_history: List[Result] = []
_history_lock = threading.Lock()


def signal_handler(signum, frame):
    """Handle Ctrl+C gracefully - actually shutdown immediately."""
    if RICH_AVAILABLE:
        console = Console()
        console.print("\n[bold yellow]⚠️  Shutdown requested. Stopping all workers...[/bold yellow]")
    else:
        print("\n⚠️  Shutdown requested. Stopping all workers...")
    
    # Set shutdown event to stop new work
    _shutdown_event.set()
    
    # Give a brief moment for threads to notice, then force exit if needed
    # We don't block here - let the main loop handle cleanup


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


# -------------------- UI --------------------
class PremiumUI:
    """Premium terminal UI with rich dashboards and animations."""

    def __init__(self):
        self.console = Console() if RICH_AVAILABLE else None
        self._progress = None
        self._live = None
        self._spinner_idx = 0

    def _get_spinner(self):
        spinner = SPINNERS[self._spinner_idx % len(SPINNERS)]
        self._spinner_idx += 1
        return spinner

    def print(self, *args, **kwargs):
        if self.console:
            self.console.print(*args, **kwargs)
        else:
            print(*args)

    def rule(self, title="", style="cyan"):
        if self.console:
            self.console.rule(f"[bold {style}]{title}[/bold {style}]")
        else:
            print(f"\n{'='*60}")
            if title:
                print(f"  {title}")
            print(f"{'='*60}\n")

    def error(self, msg: str):
        if self.console:
            self.console.print(f"[bold red]✗ {msg}[/bold red]")
        else:
            print(f"✗ {msg}")

    def success(self, msg: str):
        if self.console:
            self.console.print(f"[bold green]✓ {msg}[/bold green]")
        else:
            print(f"✓ {msg}")

    def warning(self, msg: str):
        if self.console:
            self.console.print(f"[bold yellow]⚠ {msg}[/bold yellow]")
        else:
            print(f"⚠ {msg}")

    def info(self, msg: str):
        if self.console:
            self.console.print(f"[cyan]ℹ {msg}[/cyan]")
        else:
            print(f"ℹ {msg}")

    def dim(self, msg: str):
        if self.console:
            self.console.print(f"[dim]{msg}[/dim]")
        else:
            print(msg)

    def print_banner(self):
        """Print an epic ASCII banner."""
        if not self.console:
            print("=== NGLBomb Pro ===")
            return

        banner = """
┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃  ███╗   ██╗ ██████╗ ██╗     ██████╗  ██████╗ ███╗   ███╗██████╗  ┃
┃  ████╗  ██║██╔════╝ ██║     ██╔══██╗██╔═══██╗████╗ ████║██╔══██╗ ┃
┃  ██╔██╗ ██║██║  ███╗██║     ██████╔╝██║   ██║██╔████╔██║██████╔╝ ┃
┃  ██║╚██╗██║██║   ██║██║     ██╔══██╗██║   ██║██║╚██╔╝██║██╔══██╗ ┃
┃  ██║ ╚████║╚██████╔╝███████╗██████╔╝╚██████╔╝██║ ╚═╝ ██║██████╔╝ ┃
┃  ╚═╝  ╚═══╝ ╚═════╝ ╚══════╝╚═════╝  ╚═════╝ ╚═╝     ╚═╝╚═════╝  ┃
┃                                                                  ┃
┃                [bold cyan]Educational Purpose Only[/bold cyan]  •  [dim]v1.0.0[/dim]               ┃
┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛
"""
        self.console.print(banner)

    def create_progress(self, total: int, description: str):
        if self.console and RICH_AVAILABLE:
            self._progress = Progress(
                SpinnerColumn(spinner_name=self._get_spinner()),
                TextColumn("[bold blue]{task.description}"),
                BarColumn(complete_style="green", finished_style="bright_green", bar_width=40),
                MofNCompleteColumn(),
                TaskProgressColumn(),
                TimeRemainingColumn(),
                TextColumn("[bold green]{task.fields[success]}✓[/bold green] [bold red]{task.fields[fail]}✗[/bold red] [dim]{task.fields[rps]:.1f}/s[/dim]"),
                console=self.console,
                transient=False,
            )
            task_id = self._progress.add_task(
                description, 
                total=total, 
                success=0, 
                fail=0, 
                rps=0.0
            )
            return self._progress, task_id
        return None, None

    def update_progress(self, progress, task_id, success: int, fail: int, rps: float):
        if progress and task_id is not None:
            progress.update(task_id, advance=1, success=success, fail=fail, rps=rps)

    def build_dashboard(self, stats: Stats, config: Config) -> Layout:
        """Build a full live dashboard layout."""
        layout = Layout()
        layout.split_column(
            Layout(name="header", size=5),
            Layout(name="body"),
            Layout(name="footer", size=3)
        )
        layout["body"].split_row(
            Layout(name="left", ratio=2),
            Layout(name="right", ratio=1)
        )

        # Header
        elapsed = stats.elapsed_seconds
        header_content = Group(
            Text(f"🎯 Target: ", style="cyan") + Text(f"https://ngl.link/{config.username}", style="bold white"),
            Text(f"⚡ Workers: {config.workers}  |  ⏱ Delay: {config.delay:.2f}s  |  🔄 Retries: {config.retries}", style="dim")
        )
        header = Panel(
            Align.center(header_content),
            title="[bold cyan]NGLBomb Pro[/bold cyan]",
            subtitle=f"[dim]Elapsed: {elapsed:.1f}s[/dim]",
            border_style="cyan",
            box=box.ROUNDED
        )

        # Left panel - Main stats
        stats_table = Table(box=box.SIMPLE_HEAD, expand=True, show_header=False)
        stats_table.add_column("Label", style="cyan", width=18)
        stats_table.add_column("Value", style="white")

        stats_table.add_row("Total Messages", f"[bold white]{stats.total}[/bold white] / {config.count}")
        stats_table.add_row("Successful", f"[bold green]{stats.success}[/bold green]")
        stats_table.add_row("Failed", f"[bold red]{stats.failed}[/bold red]" if stats.failed > 0 else "[dim]0[/dim]")
        stats_table.add_row("Success Rate", self._color_rate(stats.success_rate))
        stats_table.add_row("Recent Rate", self._color_rate(stats.recent_success_rate))
        stats_table.add_row("Current RPS", f"[bold yellow]{stats.current_rps:.1f}[/bold yellow] msg/s")
        stats_table.add_row("Overall RPS", f"[bold]{stats.rate_per_second:.1f}[/bold] msg/s")
        stats_table.add_row("Avg Response", f"{stats.avg_duration_ms:.0f}ms")
        stats_table.add_row("Recent Response", f"{stats.avg_recent_duration:.0f}ms")
        stats_table.add_row("Min/Max", f"[green]{stats.min_duration_ms:.0f}ms[/green] / [red]{stats.max_duration_ms:.0f}ms[/red]")

        left_panel = Panel(
            stats_table,
            title="[bold]📊 Statistics[/bold]",
            border_style="blue",
            box=box.ROUNDED
        )

        # Right panel - Errors
        if stats.errors_by_type:
            error_table = Table(box=box.SIMPLE_HEAD, expand=True, show_header=False)
            error_table.add_column("Error", style="red")
            error_table.add_column("Count", style="yellow", justify="right")

            for err_type, count in sorted(stats.errors_by_type.items(), key=lambda x: -x[1]):
                pct = (count / max(stats.total, 1)) * 100
                error_table.add_row(err_type, f"{count} ({pct:.0f}%)")

            right_panel = Panel(
                error_table,
                title="[bold red]⚠ Errors[/bold red]",
                border_style="red",
                box=box.ROUNDED
            )
        else:
            right_panel = Panel(
                Align.center(Text("✓ No errors yet", style="green")),
                title="[bold green]✓ Errors[/bold green]",
                border_style="green",
                box=box.ROUNDED
            )

        # Footer
        footer_text = Text()
        if config.dry_run:
            footer_text.append("🧪 DRY RUN MODE ", style="bold yellow")
        if config.adaptive_delay:
            footer_text.append(f"| Adaptive Delay: ON ", style="dim")
        footer_text.append(f"| Current Delay: {_current_delay:.2f}s ", style="dim")
        footer_text.append("| Press Ctrl+C to stop gracefully", style="dim")

        footer = Panel(
            Align.center(footer_text),
            border_style="dim",
            box=box.SIMPLE
        )

        layout["header"].update(header)
        layout["left"].update(left_panel)
        layout["right"].update(right_panel)
        layout["footer"].update(footer)

        return layout

    def _color_rate(self, rate: float) -> str:
        if rate >= 95:
            return f"[bold green]{rate:.1f}%[/bold green]"
        elif rate >= 80:
            return f"[bold yellow]{rate:.1f}%[/bold yellow]"
        elif rate >= 50:
            return f"[bold orange3]{rate:.1f}%[/bold orange3]"
        else:
            return f"[bold red]{rate:.1f}%[/bold red]"

    def print_summary(self, stats: Stats, results: List[Result], config: Config):
        self.rule("SESSION SUMMARY", "green")

        if self.console and RICH_AVAILABLE:
            # Main stats panel
            main_table = Table(box=box.ROUNDED, title="[bold cyan]📊 Final Statistics[/bold cyan]", expand=True)
            main_table.add_column("Metric", style="cyan", justify="right", width=20)
            main_table.add_column("Value", style="white")

            main_table.add_row("🎯 Target", f"https://ngl.link/{config.username}")
            main_table.add_row("📨 Total Messages", str(stats.total))
            main_table.add_row("✅ Successful", f"[bold green]{stats.success}[/bold green]")
            main_table.add_row("❌ Failed", f"[bold red]{stats.failed}[/bold red]" if stats.failed > 0 else "[dim]0[/dim]")
            main_table.add_row("📈 Success Rate", self._color_rate(stats.success_rate))
            main_table.add_row("⏱ Elapsed Time", f"{stats.elapsed_seconds:.2f}s")
            main_table.add_row("⚡ Avg Rate", f"[bold]{stats.rate_per_second:.1f}[/bold] msg/s")
            main_table.add_row("📊 Avg Response", f"{stats.avg_duration_ms:.0f}ms")
            main_table.add_row("🔻 Min Response", f"[green]{stats.min_duration_ms:.0f}ms[/green]")
            main_table.add_row("🔺 Max Response", f"[red]{stats.max_duration_ms:.0f}ms[/red]")
            main_table.add_row("📤 Bytes Sent", f"{stats.total_bytes_sent:,} bytes")

            self.console.print(main_table)

            # Error breakdown
            if stats.errors_by_type:
                err_table = Table(box=box.ROUNDED, title="[bold red]⚠ Error Breakdown[/bold red]", expand=True)
                err_table.add_column("Error Type", style="red")
                err_table.add_column("Count", style="yellow", justify="right")
                err_table.add_column("Percentage", style="white", justify="right")
                err_table.add_column("Bar", style="red")

                max_count = max(stats.errors_by_type.values())
                for err_type, count in sorted(stats.errors_by_type.items(), key=lambda x: -x[1]):
                    pct = (count / stats.total) * 100
                    bar_len = int((count / max_count) * 20)
                    bar = "█" * bar_len
                    err_table.add_row(err_type, str(count), f"{pct:.1f}%", bar)

                self.console.print(err_table)

            # Recent failures
            failures = [r for r in results if not r.ok][:15]
            if failures:
                fail_table = Table(box=box.ROUNDED, title="[bold red]📋 Recent Failures[/bold red]", expand=True)
                fail_table.add_column("#", style="dim", width=6)
                fail_table.add_column("Error", style="red")
                fail_table.add_column("Status", style="yellow", width=10)
                fail_table.add_column("Attempts", style="cyan", width=8)
                fail_table.add_column("Duration", style="dim", width=10)
                fail_table.add_column("Proxy", style="dim")

                for r in failures:
                    fail_table.add_row(
                        str(r.index), 
                        r.error or "Unknown", 
                        str(r.status) if r.status else "-",
                        str(r.attempt),
                        f"{r.duration_ms:.0f}ms",
                        r.proxy_used or "-"
                    )

                self.console.print(fail_table)

            # Success distribution
            if len(results) > 10:
                self._print_sparkline(results)
        else:
            print(f"\nTarget: https://ngl.link/{config.username}")
            print(f"Total: {stats.total}")
            print(f"Success: {stats.success}")
            print(f"Failed: {stats.failed}")
            print(f"Success Rate: {stats.success_rate:.1f}%")
            print(f"Elapsed: {stats.elapsed_seconds:.2f}s")
            print(f"Rate: {stats.rate_per_second:.1f} msg/s")
            if stats.errors_by_type:
                print(f"\nErrors:")
                for err_type, count in stats.errors_by_type.items():
                    print(f"  {err_type}: {count}")

    def _print_sparkline(self, results: List[Result]):
        """Print a visual sparkline of success/failure."""
        chunk_size = max(1, len(results) // 50)
        chunks = [results[i:i+chunk_size] for i in range(0, len(results), chunk_size)]

        line = ""
        for chunk in chunks:
            ok_count = sum(1 for r in chunk if r.ok)
            ratio = ok_count / len(chunk)
            if ratio == 1.0:
                line += "[green]█[/green]"
            elif ratio >= 0.7:
                line += "[yellow]▓[/yellow]"
            elif ratio >= 0.3:
                line += "[orange3]▒[/orange3]"
            else:
                line += "[red]░[/red]"

        self.console.print(f"\n[dim]Success Map (each block = ~{chunk_size} requests):[/dim]")
        self.console.print(line)
        self.console.print("[green]█[/green] 100%  [yellow]▓[/yellow] 70%+  [orange3]▒[/orange3] 30%+  [red]░[/red] <30%")

    def print_config_tree(self, config: Config):
        """Print configuration as a tree."""
        if not self.console:
            return

        tree = Tree("[bold cyan]⚙ Configuration[/bold cyan]")
        tree.add(f"[cyan]Username:[/cyan] [white]{config.username}[/white]")
        tree.add(f"[cyan]Count:[/cyan] [white]{config.count}[/white]")
        tree.add(f"[cyan]Workers:[/cyan] [white]{config.workers}[/white]")
        tree.add(f"[cyan]Delay:[/cyan] [white]{config.delay}s[/white]")
        tree.add(f"[cyan]Timeout:[/cyan] [white]{config.timeout}s[/white]")
        tree.add(f"[cyan]Retries:[/cyan] [white]{config.retries}[/white]")
        tree.add(f"[cyan]Adaptive Delay:[/cyan] [green]ON[/green]" if config.adaptive_delay else "[red]OFF[/red]")
        tree.add(f"[cyan]UA Rotation:[/cyan] [green]ON[/green]" if config.rotate_ua else "[red]OFF[/red]")
        tree.add(f"[cyan]Questions:[/cyan] [white]{len(config.questions)}[/white]")
        if config.dry_run:
            tree.add("[bold yellow]🧪 DRY RUN MODE[/bold yellow]")
        if config.use_proxy:
            tree.add(f"[cyan]Proxies:[/cyan] [white]{len(config.proxy_list)}[/white]")

        self.console.print(tree)


ui = PremiumUI()


# -------------------- Connection Pool --------------------
class ConnectionPool:
    """Thread-safe connection pool for urllib.request."""

    def __init__(self, maxsize: int = 10, timeout: float = 10.0):
        self.maxsize = maxsize
        self.timeout = timeout
        self._connections: Dict[str, List[http.client.HTTPConnection]] = {}
        self._lock = threading.Lock()
        self._condition = threading.Condition(self._lock)

    def _get_key(self, host: str, port: int, is_https: bool) -> str:
        scheme = "https" if is_https else "http"
        return f"{scheme}://{host}:{port}"

    def _create_connection(self, host: str, port: int, is_https: bool, timeout: float) -> http.client.HTTPConnection:
        if is_https:
            return http.client.HTTPSConnection(host, port, timeout=timeout)
        return http.client.HTTPConnection(host, port, timeout=timeout)

    def get_connection(self, host: str, port: int, is_https: bool) -> http.client.HTTPConnection:
        key = self._get_key(host, port, is_https)
        with self._condition:
            while True:
                if key in self._connections and self._connections[key]:
                    conn = self._connections[key].pop()
                    try:
                        conn.sock.settimeout(0.1)
                        conn.sock.recv(1, socket.MSG_PEEK)
                        conn.sock.settimeout(self.timeout)
                        return conn
                    except (OSError, AttributeError):
                        try:
                            conn.close()
                        except Exception:
                            pass
                        return self._create_connection(host, port, is_https, self.timeout)

                total_connections = sum(len(conns) for conns in self._connections.values())
                if total_connections < self.maxsize:
                    return self._create_connection(host, port, is_https, self.timeout)

                self._condition.wait(timeout=1.0)

    def release_connection(self, conn: http.client.HTTPConnection, host: str, port: int, is_https: bool):
        key = self._get_key(host, port, is_https)
        with self._condition:
            if key not in self._connections:
                self._connections[key] = []
            if len(self._connections[key]) < self.maxsize:
                self._connections[key].append(conn)
            else:
                try:
                    conn.close()
                except Exception:
                    pass
            self._condition.notify()

    def close_all(self):
        with self._lock:
            for key, conns in self._connections.items():
                for conn in conns:
                    try:
                        conn.close()
                    except Exception:
                        pass
            self._connections.clear()


_connection_pool = ConnectionPool(maxsize=20)


# -------------------- Utils --------------------
def validate_username(username: str) -> bool:
    return bool(re.fullmatch(r"[A-Za-z0-9_.-]{2,40}", username))


def random_device_id(length: int = 36) -> str:
    chars = string.ascii_letters + string.digits
    return "".join(random.choice(chars) for _ in range(length))


def get_message(config: Config, index: int) -> str:
    template = config.questions[(index - 1) % len(config.questions)]
    return template.format(i=index, time=datetime.now().strftime("%H:%M:%S"))


def get_random_user_agent(config: Config) -> str:
    if config.rotate_ua:
        return random.choice(USER_AGENTS)
    return DEFAULT_USER_AGENT


def get_proxy(config: Config) -> Optional[str]:
    if config.use_proxy and config.proxy_list:
        return random.choice(config.proxy_list)
    return None


def make_payload(config: Config, message: str) -> Tuple[bytes, int]:
    payload = {
        "username": config.username,
        "question": message,
        "deviceId": random_device_id(),
        "gameSlug": "",
        "referrer": "",
    }
    encoded = urllib.parse.urlencode(payload).encode()
    return encoded, len(encoded)


def build_opener(proxy: Optional[str] = None) -> urllib.request.OpenerDirector:
    handlers = []
    if proxy:
        proxy_handler = urllib.request.ProxyHandler({
            'http': proxy,
            'https': proxy,
        })
        handlers.append(proxy_handler)

    import http.client
    http.client.HTTPConnection._http_vsn = 11
    http.client.HTTPConnection._http_vsn_str = 'HTTP/1.1'

    return urllib.request.build_opener(*handlers)


def update_stats(result: Result, bytes_sent: int = 0, bytes_received: int = 0):
    with _stats_lock:
        _stats.total += 1
        _stats.success_history.append(result.ok)
        _stats.duration_history.append(result.duration_ms)

        if result.ok:
            _stats.success += 1
        else:
            _stats.failed += 1
            err_type = result.error or "Unknown"
            if "429" in err_type:
                err_key = "Rate Limited (429)"
            elif "403" in err_type:
                err_key = "Forbidden (403)"
            elif "500" in err_type:
                err_key = "Server Error (500)"
            elif "502" in err_type:
                err_key = "Bad Gateway (502)"
            elif "503" in err_type:
                err_key = "Service Unavailable (503)"
            elif "504" in err_type:
                err_key = "Gateway Timeout (504)"
            elif "HTTP" in err_type:
                err_key = "HTTP Error"
            elif "timeout" in err_type.lower():
                err_key = "Timeout"
            elif "connection" in err_type.lower():
                err_key = "Connection"
            elif "ssl" in err_type.lower():
                err_key = "SSL"
            else:
                err_key = "Other"
            _stats.errors_by_type[err_key] = _stats.errors_by_type.get(err_key, 0) + 1

        if result.duration_ms > 0:
            _stats.avg_duration_ms = ((_stats.avg_duration_ms * (_stats.total - 1)) + result.duration_ms) / _stats.total
            _stats.min_duration_ms = min(_stats.min_duration_ms, result.duration_ms)
            _stats.max_duration_ms = max(_stats.max_duration_ms, result.duration_ms)

        _stats.total_bytes_sent += bytes_sent
        _stats.total_bytes_received += bytes_received

        # Update RPS
        now = time.time()
        if _stats.last_update_time > 0:
            dt = now - _stats.last_update_time
            if dt > 0:
                _stats.current_rps = 1.0 / dt
        _stats.last_update_time = now


def adaptive_delay_adjustment(result: Result, config: Config):
    if not config.adaptive_delay:
        return

    global _current_delay
    with _delay_lock:
        if result.ok:
            if result.duration_ms < 500:
                _current_delay = max(config.min_delay, _current_delay * 0.95)
            elif result.duration_ms > 3000:
                _current_delay = min(config.max_delay, _current_delay * 1.2)
        else:
            if "429" in (result.error or ""):
                _current_delay = min(config.max_delay, _current_delay * 2.5)
            elif "timeout" in (result.error or "").lower():
                _current_delay = min(config.max_delay, _current_delay * 1.5)
            else:
                _current_delay = min(config.max_delay, _current_delay * 1.2)


# -------------------- Core Logic --------------------
_opener_cache: Dict[Optional[str], urllib.request.OpenerDirector] = {}
_opener_lock = threading.Lock()


def get_cached_opener(proxy: Optional[str] = None) -> urllib.request.OpenerDirector:
    with _opener_lock:
        if proxy not in _opener_cache:
            _opener_cache[proxy] = build_opener(proxy)
        return _opener_cache[proxy]


def post_once(config: Config, payload: bytes, user_agent: str, proxy: Optional[str] = None) -> int:
    safe_username = urllib.parse.quote(config.username, safe="")

    req = urllib.request.Request(
        config.endpoint,
        data=payload,
        headers={
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Origin": "https://ngl.link",
            "Referer": f"https://ngl.link/{safe_username}",
            "User-Agent": user_agent,
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "X-Requested-With": "XMLHttpRequest",
            **config.custom_headers,
        },
        method="POST",
    )

    opener = get_cached_opener(proxy)
    with opener.open(req, timeout=config.timeout) as res:
        return int(res.getcode())


def send_message(config: Config, index: int) -> Result:
    if _shutdown_event.is_set():
        return Result(index, False, None, "Shutdown requested", 0, 0, datetime.now().isoformat(), "", "")

    message = get_message(config, index)
    payload, bytes_sent = make_payload(config, message)
    user_agent = get_random_user_agent(config)
    proxy = get_proxy(config)

    start_time = time.time()
    attempt = 0

    while True:
        if _shutdown_event.is_set():
            duration_ms = (time.time() - start_time) * 1000
            return Result(index, False, None, "Shutdown requested", duration_ms, attempt, datetime.now().isoformat(), proxy, user_agent)

        try:
            if config.dry_run:
                time.sleep(random.uniform(0.01, 0.05))
                status = 200
            else:
                status = post_once(config, payload, user_agent, proxy)

            duration_ms = (time.time() - start_time) * 1000
            result = Result(index, 200 <= status < 300, status, None, duration_ms, attempt + 1, datetime.now().isoformat(), proxy, user_agent)
            update_stats(result, bytes_sent, 0)
            adaptive_delay_adjustment(result, config)

            with _history_lock:
                _results_history.append(result)

            return result

        except urllib.error.HTTPError as e:
            err = f"HTTP {e.code}"
            if e.code == 429:
                err = f"HTTP 429 (Rate Limited)"
            elif e.code == 403:
                err = f"HTTP 403 (Forbidden)"
            elif e.code == 500:
                err = f"HTTP 500 (Server Error)"
            elif e.code == 502:
                err = f"HTTP 502 (Bad Gateway)"
            elif e.code == 503:
                err = f"HTTP 503 (Service Unavailable)"
            elif e.code == 504:
                err = f"HTTP 504 (Gateway Timeout)"
        except urllib.error.URLError as e:
            err = f"URL Error: {e.reason}"
        except TimeoutError:
            err = "Timeout"
        except socket.timeout:
            err = "Socket Timeout"
        except socket.error as e:
            err = f"Socket Error: {e}"
        except Exception as e:
            err = str(e) or "Unknown error"

        if attempt >= config.retries:
            duration_ms = (time.time() - start_time) * 1000
            result = Result(index, False, None, err, duration_ms, attempt + 1, datetime.now().isoformat(), proxy, user_agent)
            update_stats(result, bytes_sent, 0)
            adaptive_delay_adjustment(result, config)

            with _history_lock:
                _results_history.append(result)

            return result

        attempt += 1
        backoff = min(config.delay * (2 ** attempt), MAX_BACKOFF_SECONDS)
        jitter = random.uniform(0, max(config.delay, MIN_JITTER_SECONDS))
        sleep_time = backoff + jitter

        if _shutdown_event.is_set():
            duration_ms = (time.time() - start_time) * 1000
            return Result(index, False, None, f"Shutdown during retry (attempt {attempt})", duration_ms, attempt, datetime.now().isoformat(), proxy, user_agent)

        time.sleep(sleep_time)


def run_with_dashboard(config: Config) -> List[Result]:
    """Run with full live dashboard - FIXED: Actually stops on shutdown."""
    results: List[Result] = []
    lock = threading.Lock()

    global _current_delay
    _current_delay = config.delay
    _stats.start_time = time.time()

    def task(i: int):
        return send_message(config, i)

    # Pre-create all futures
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=config.workers)
    futures = [executor.submit(task, i) for i in range(1, config.count + 1)]
    
    try:
        if config.show_dashboard and RICH_AVAILABLE and ui.console:
            # Build and run live dashboard
            with Live(refresh_per_second=4, console=ui.console, screen=False) as live:
                completed = 0
                for f in concurrent.futures.as_completed(futures):
                    if _shutdown_event.is_set():
                        # Cancel remaining futures immediately
                        for fut in futures:
                            fut.cancel()
                        break
                    
                    r = f.result()
                    with lock:
                        results.append(r)
                        completed += 1

                    # Update dashboard every few completions or on errors
                    if completed % 3 == 0 or not r.ok or completed == config.count:
                        live.update(ui.build_dashboard(_stats, config))
        else:
            # Fallback to progress bar mode
            progress, task_id = ui.create_progress(config.count, f"[cyan]Sending to {config.username}...")

            if progress and task_id is not None:
                with progress:
                    for f in concurrent.futures.as_completed(futures):
                        if _shutdown_event.is_set():
                            for fut in futures:
                                fut.cancel()
                            break
                        
                        r = f.result()
                        with lock:
                            results.append(r)
                            ui.update_progress(progress, task_id, _stats.success, _stats.failed, _stats.current_rps)
            else:
                # Plain mode
                for i, f in enumerate(concurrent.futures.as_completed(futures), 1):
                    if _shutdown_event.is_set():
                        for fut in futures:
                            fut.cancel()
                        break
                    
                    r = f.result()
                    with lock:
                        results.append(r)
                    if i % 10 == 0 or i == config.count:
                        print(f"  [{i}/{config.count}] Success: {_stats.success} | Failed: {_stats.failed} | Rate: {_stats.current_rps:.1f}/s")
    finally:
        # CRITICAL FIX: Actually shutdown the executor with a timeout
        # Don't wait forever for threads to finish
        executor.shutdown(wait=False, cancel_futures=True)
        
        # Give threads a brief moment to notice shutdown, then force kill
        if _shutdown_event.is_set():
            time.sleep(0.5)  # Brief grace period
            
    _stats.end_time = time.time()
    return sorted(results, key=lambda x: x.index)


# -------------------- Export --------------------
def export_results(results: List[Result], config: Config):
    if not config.export_format or not config.export_path:
        return

    path = Path(config.export_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    if config.export_format.lower() == "json":
        data = {
            "meta": {
                "tool": "NGLBomb Pro",
                "version": "1.0.0",
                "timestamp": datetime.now().isoformat(),
            },
            "config": {
                "username": config.username,
                "count": config.count,
                "workers": config.workers,
                "delay": config.delay,
                "timeout": config.timeout,
                "retries": config.retries,
                "endpoint": config.endpoint,
                "questions": config.questions,
                "adaptive_delay": config.adaptive_delay,
                "dry_run": config.dry_run,
            },
            "stats": {
                "total": _stats.total,
                "success": _stats.success,
                "failed": _stats.failed,
                "success_rate": _stats.success_rate,
                "elapsed_seconds": _stats.elapsed_seconds,
                "rate_per_second": _stats.rate_per_second,
                "avg_duration_ms": _stats.avg_duration_ms,
                "min_duration_ms": _stats.min_duration_ms,
                "max_duration_ms": _stats.max_duration_ms,
                "errors_by_type": _stats.errors_by_type,
                "total_bytes_sent": _stats.total_bytes_sent,
            },
            "results": [
                {
                    "index": r.index,
                    "ok": r.ok,
                    "status": r.status,
                    "error": r.error,
                    "duration_ms": r.duration_ms,
                    "attempts": r.attempt,
                    "timestamp": r.timestamp,
                    "proxy": r.proxy_used,
                    "user_agent": r.user_agent,
                }
                for r in results
            ]
        }
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

    elif config.export_format.lower() == "csv":
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["index", "ok", "status", "error", "duration_ms", "attempts", "timestamp", "proxy", "user_agent"])
            for r in results:
                writer.writerow([r.index, r.ok, r.status, r.error, r.duration_ms, r.attempt, r.timestamp, r.proxy_used, r.user_agent])

    ui.success(f"Results exported to {path}")


# -------------------- Config Management --------------------
def save_config(config: Config):
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    data = {
        "username": config.username,
        "count": config.count,
        "workers": config.workers,
        "delay": config.delay,
        "timeout": config.timeout,
        "retries": config.retries,
        "endpoint": config.endpoint,
        "questions": config.questions,
    }
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def load_config() -> Optional[Dict]:
    if CONFIG_FILE.exists():
        with open(CONFIG_FILE, encoding="utf-8") as f:
            return json.load(f)
    return None


# -------------------- Interactive Input --------------------
def ask_input(prompt: str, default=None, cast=str, validate: Optional[Callable] = None):
    while True:
        try:
            if default is not None:
                value = input(f"{prompt} [{default}]: ").strip()
            else:
                value = input(f"{prompt}: ").strip()
        except (EOFError, KeyboardInterrupt):
            return default

        if not value:
            return default

        try:
            result = cast(value)
            if validate and not validate(result):
                ui.error("Invalid input. Please try again.")
                continue
            return result
        except (ValueError, TypeError):
            ui.error("Invalid format. Please try again.")
            continue


def rich_ask_input(prompt: str, default=None, cast=str, validate: Optional[Callable] = None):
    """Use Rich prompts when available."""
    if not RICH_AVAILABLE:
        return ask_input(prompt, default, cast, validate)

    while True:
        try:
            if default is not None:
                value = Prompt.ask(f"[cyan]{prompt}[/cyan]", default=str(default))
            else:
                value = Prompt.ask(f"[cyan]{prompt}[/cyan]")
            value = value.strip()
            if not value:
                return default

            result = cast(value)
            if validate and not validate(result):
                ui.error("Invalid input. Please try again.")
                continue
            return result
        except (ValueError, TypeError, KeyboardInterrupt):
            return default


def rich_confirm(prompt: str, default: bool = True) -> bool:
    if RICH_AVAILABLE:
        try:
            return Confirm.ask(f"[cyan]{prompt}[/cyan]", default=default)
        except KeyboardInterrupt:
            return default
    else:
        val = ask_input(f"{prompt} (y/n)", "y" if default else "n", str.lower)
        return val in ("y", "yes")


def load_questions_interactive() -> List[str]:
    use_file = rich_confirm("Use question.json?", True)

    if use_file:
        if os.path.exists("question.json"):
            try:
                with open("question.json", encoding="utf-8") as f:
                    data = json.load(f)
                if isinstance(data, list) and data:
                    questions = [str(x) for x in data if str(x).strip()]
                    if questions:
                        ui.success(f"Loaded {len(questions)} questions from question.json")
                        return questions
                ui.warning("question.json is invalid or empty")
            except Exception as e:
                ui.error(f"Failed to load question.json: {e}")
        else:
            ui.warning("question.json not found")

    # Manual input
    questions = []
    ui.info("Enter questions (use {i} for index, {time} for timestamp). Empty line to finish.")
    while True:
        q = ask_input(f"Question {len(questions) + 1}", "")
        if not q:
            break
        questions.append(q)

    if not questions:
        questions = ["hello #{i}"]
        ui.info("Using default question: hello #{i}")

    return questions


def interactive_config() -> Config:
    ui.rule("Interactive Setup", "cyan")

    prev_config = load_config()

    # Username
    default_user = prev_config.get("username", "") if prev_config else ""
    while True:
        username = rich_ask_input("Username", default_user, str.lower)
        if username and validate_username(username):
            break
        ui.error("Username must be 2-40 chars, alphanumeric + _ . -")

    # Questions
    questions = load_questions_interactive()

    # Count
    default_count = prev_config.get("count", 10) if prev_config else 10
    count = rich_ask_input("Count", default_count, int, lambda x: x > 0)

    # Workers
    default_workers = prev_config.get("workers", 4) if prev_config else 4
    workers = rich_ask_input("Workers", default_workers, int, lambda x: 1 <= x <= 50)

    # Delay
    default_delay = prev_config.get("delay", 0.5) if prev_config else 0.5
    delay = rich_ask_input("Delay (seconds)", default_delay, float, lambda x: x >= 0)

    # Timeout
    default_timeout = prev_config.get("timeout", 10.0) if prev_config else 10.0
    timeout = rich_ask_input("Timeout", default_timeout, float, lambda x: x > 0)

    # Retries
    default_retries = prev_config.get("retries", 3) if prev_config else 3
    retries = rich_ask_input("Retries", default_retries, int, lambda x: x >= 0)

    # Advanced options
    ui.info("Advanced options (press Enter to skip):")

    adaptive = rich_confirm("Adaptive delay?", True)
    rotate_ua = rich_confirm("Rotate User-Agent?", True)
    show_dash = rich_confirm("Show live dashboard?", True)

    export_fmt = rich_ask_input("Export format (json/csv/none)", "none", str.lower)
    export_path = None
    if export_fmt in ("json", "csv"):
        export_path = rich_ask_input("Export path", f"ngl_results.{export_fmt}")

    dry_run = rich_confirm("Dry run?", False)
    if dry_run:
        ui.warning("Dry run mode - no actual requests will be sent")

    config = Config(
        username=username,
        count=max(1, count),
        workers=max(1, workers),
        delay=max(0, delay),
        timeout=timeout,
        retries=max(0, retries),
        endpoint=DEFAULT_ENDPOINT,
        questions=questions,
        adaptive_delay=adaptive,
        rotate_ua=rotate_ua,
        show_dashboard=show_dash,
        export_format=export_fmt if export_fmt in ("json", "csv") else None,
        export_path=export_path,
        dry_run=dry_run,
    )

    try:
        save_config(config)
    except Exception:
        pass

    return config


# -------------------- CLI Args --------------------
def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="NGLBomb - Ultimate Rich CLI Utility",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                          # Interactive mode with rich UI
  %(prog)s -u john -c 50           # Quick send 50 messages
  %(prog)s -u john -c 100 -w 8 -d 0.1  # Fast mode with 8 workers
  %(prog)s -u john -c 10 --dry-run   # Test without sending
  %(prog)s -u john -c 50 -e json -o results.json  # Export to JSON
  %(prog)s -u john -c 100 --proxy http://proxy:8080  # Use proxy
  %(prog)s -u john -c 50 --no-dashboard  # Disable live dashboard
        """
    )

    parser.add_argument("-u", "--username", help="Target username")
    parser.add_argument("-c", "--count", type=int, default=10, help="Number of messages (default: 10)")
    parser.add_argument("-w", "--workers", type=int, default=4, help="Thread workers (default: 4)")
    parser.add_argument("-d", "--delay", type=float, default=0.5, help="Base delay in seconds (default: 0.5)")
    parser.add_argument("-t", "--timeout", type=float, default=10.0, help="Request timeout (default: 10)")
    parser.add_argument("-r", "--retries", type=int, default=3, help="Max retries (default: 3)")
    parser.add_argument("-q", "--questions", nargs="+", help="Questions to send (use {i} for index)")
    parser.add_argument("-f", "--question-file", help="JSON file with questions array")
    parser.add_argument("--dry-run", action="store_true", help="Simulate without sending")
    parser.add_argument("-e", "--export", choices=["json", "csv"], help="Export format")
    parser.add_argument("-o", "--output", help="Export file path")
    parser.add_argument("--no-adaptive", action="store_true", help="Disable adaptive delay")
    parser.add_argument("--no-rotate-ua", action="store_true", help="Disable User-Agent rotation")
    parser.add_argument("--no-dashboard", action="store_true", help="Disable live dashboard")
    parser.add_argument("--proxy", help="Proxy URL (http://host:port)")
    parser.add_argument("--proxy-file", help="File with proxy list (one per line)")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    parser.add_argument("--endpoint", default=DEFAULT_ENDPOINT, help="Custom API endpoint")

    return parser


def config_from_args(args) -> Optional[Config]:
    if not args.username:
        return None

    username = args.username.strip().lower()
    if not validate_username(username):
        ui.error(f"Invalid username: {username}")
        return None

    # Load questions
    questions = []
    if args.question_file:
        try:
            with open(args.question_file, encoding="utf-8") as f:
                data = json.load(f)
            questions = [str(x) for x in data if str(x).strip()]
        except Exception as e:
            ui.error(f"Failed to load question file: {e}")
            return None
    elif args.questions:
        questions = list(args.questions)
    else:
        if os.path.exists("question.json"):
            try:
                with open("question.json", encoding="utf-8") as f:
                    data = json.load(f)
                questions = [str(x) for x in data if str(x).strip()]
            except Exception:
                pass

    if not questions:
        questions = ["hello #{i}"]

    # Load proxies
    proxy_list = []
    if args.proxy:
        proxy_list = [args.proxy]
    if args.proxy_file:
        try:
            with open(args.proxy_file, encoding="utf-8") as f:
                proxy_list = [line.strip() for line in f if line.strip()]
        except Exception as e:
            ui.error(f"Failed to load proxy file: {e}")

    return Config(
        username=username,
        count=max(1, args.count),
        workers=max(1, args.workers),
        delay=max(0, args.delay),
        timeout=args.timeout,
        retries=max(0, args.retries),
        endpoint=args.endpoint,
        questions=questions,
        adaptive_delay=not args.no_adaptive,
        rotate_ua=not args.no_rotate_ua,
        show_dashboard=not args.no_dashboard,
        export_format=args.export,
        export_path=args.output,
        dry_run=args.dry_run,
        use_proxy=bool(proxy_list),
        proxy_list=proxy_list,
        verbose=args.verbose,
    )


# -------------------- Main --------------------
def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    # Banner
    if RICH_AVAILABLE:
        ui.print_banner()
        ui.console.print(Align.center(
            Text("Ultimate Edition  •  ", style="dim") + Text("v1.0.0", style="bold cyan")
        ))
        ui.console.print()

        if not RICH_AVAILABLE:
            ui.warning("Rich not installed. Install with: pip install rich")
    else:
        print("=== NGLBomb ===")
        print("Ultimate Edition  •  v1.0.0")
        print()
        print("Install rich for premium UI: pip install rich")
        print()

    # Get config
    config = config_from_args(args)
    if config is None:
        config = interactive_config()

    # Validate
    if not config.questions:
        ui.error("No questions provided!")
        return 1

    # Show config
    ui.rule("Configuration", "blue")
    if RICH_AVAILABLE:
        ui.print_config_tree(config)
    else:
        ui.info(f"Target: https://ngl.link/{config.username}")
        ui.info(f"Messages: {config.count}")
        ui.info(f"Workers: {config.workers}")
        ui.info(f"Delay: {config.delay}s")
        ui.info(f"Retries: {config.retries}")
        ui.info(f"Timeout: {config.timeout}s")
        ui.info(f"Adaptive Delay: {'Yes' if config.adaptive_delay else 'No'}")
        ui.info(f"UA Rotation: {'Yes' if config.rotate_ua else 'No'}")
        ui.info(f"Dashboard: {'Yes' if config.show_dashboard else 'No'}")
        ui.info(f"Questions: {len(config.questions)}")
        if config.dry_run:
            ui.warning("DRY RUN MODE")
        if config.use_proxy:
            ui.info(f"Proxies: {len(config.proxy_list)}")

    # Confirm
    if not args.username:
        if not rich_confirm("\nProceed?", True):
            ui.info("Cancelled.")
            return 0

    ui.rule("Execution", "green")

    # Run
    if config.dry_run:
        ui.warning("Running in dry-run mode...")

    results = run_with_dashboard(config)

    # Summary
    ui.print_summary(_stats, results, config)

    # Export
    if config.export_format:
        export_results(results, config)

    # Cleanup
    _connection_pool.close_all()

    # Final status
    if _stats.success_rate >= 95:
        ui.success("🎉 Session completed successfully!")
    elif _stats.success_rate >= 70:
        ui.warning("⚠️  Session completed with some issues.")
    else:
        ui.error("❌ Session completed with many failures.")

    return 0 if all(r.ok for r in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())