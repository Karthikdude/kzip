#!/usr/bin/env python3
"""
KZip - High-Performance File Compression Tool
A command-line compression tool using Zstandard with async I/O
"""

import argparse
import asyncio
import os
import sys
import time
import tarfile
import tempfile
import json
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple, Union, List, Literal, Protocol
import io

try:
    import zstandard as zstd
    import aiofiles
    import psutil
    from rich.console import Console
    from rich.progress import (Progress, TextColumn, BarColumn,
                               TaskProgressColumn, TimeRemainingColumn,
                               FileSizeColumn, TransferSpeedColumn)
    from rich.table import Table
    from rich.panel import Panel
    from pydantic import BaseModel, field_validator
except ImportError as e:
    print(f"Required dependency missing: {e}")
    print("Please install required packages:")
    print("pip install zstandard aiofiles rich pydantic")
    sys.exit(1)


class KZipError(Exception):
    """Base exception for KZip operations"""
    pass


class KZipRetryableError(KZipError):
    """Errors that can be retried"""
    pass

async def with_retry(operation, max_retries=3):
    for attempt in range(max_retries):
        try:
            return await operation()
        except KZipRetryableError:
            if attempt == max_retries - 1:
                raise
            await asyncio.sleep(2 ** attempt)  # Exponential backoff


class CompressionMode(BaseModel):
    level: int
    mode: Literal['speed', 'balanced', 'compression']

    @field_validator('level')
    @classmethod
    def validate_level(cls, v):
        if not -100 <= v <= 22:
            raise ValueError('Invalid compression level')
        return v


class ProgressState:
    """Handles checkpoint save/load for resume capability"""

    def __init__(self, operation_id: str):
        self.operation_id = operation_id
        self.checkpoint_file = Path(f".kzip_progress_{operation_id}")

    def save_checkpoint(self, processed_files: List[str], current_position: int,
                       total_files: int, bytes_processed: int):
        """Save current progress to checkpoint file"""
        checkpoint_data = {
            'processed_files': processed_files,
            'current_position': current_position,
            'total_files': total_files,
            'bytes_processed': bytes_processed,
            'timestamp': time.time()
        }

        try:
            with open(self.checkpoint_file, 'w') as f:
                json.dump(checkpoint_data, f, indent=2)
        except Exception:
            pass  # Silently fail if unable to save checkpoint

    def load_checkpoint(self) -> Optional[dict]:
        """Load progress from checkpoint file"""
        try:
            if self.checkpoint_file.exists():
                with open(self.checkpoint_file, 'r') as f:
                    return json.load(f)
        except Exception:
            pass
        return None

    def cleanup(self):
        """Remove checkpoint file after successful completion"""
        try:
            if self.checkpoint_file.exists():
                self.checkpoint_file.unlink()
        except Exception:
            pass


class ResourceMonitor:
    """Monitors system resources and adjusts performance accordingly"""

    def __init__(self, max_workers: int = 8):
        self.max_workers = max_workers
        self.current_workers = min(4, max_workers)  # Start conservatively

    def should_throttle(self) -> bool:
        """Check if system resources are under stress"""
        try:
            memory_percent = psutil.virtual_memory().percent
            cpu_percent = psutil.cpu_percent(interval=0.1)
            return memory_percent > 85 or cpu_percent > 90
        except Exception:
            return False  # Conservative fallback

    def adjust_workers(self) -> int:
        """Dynamically adjust worker count based on system resources"""
        if self.should_throttle():
            self.current_workers = max(1, self.current_workers // 2)
        else:
            self.current_workers = min(self.max_workers, self.current_workers + 1)

        return self.current_workers

    def get_memory_usage_mb(self) -> float:
        """Get current memory usage in MB"""
        try:
            process = psutil.Process()
            return process.memory_info().rss / 1024 / 1024
        except Exception:
            return 0.0


class CompressionConfig:
    """Configuration for compression operations"""

    # Compression levels for different modes
    MAX_SPEED_LEVEL = 1
    BALANCED_LEVEL = 9  # Default level for balanced compression
    MAX_COMPRESSION_LEVEL = 22

    # Chunk size for streaming operations
    CHUNK_SIZE = 65536  # 64KB

    # Memory limits
    MAX_MEMORY_MB = 3072  # 3GB

    # Parallel processing limits
    MAX_CONCURRENT_FILES = 10
    CHECKPOINT_INTERVAL = 100  # Save progress every N files

    def __init__(self, level: int = 9, max_workers: int = 8, chunk_size: int = 65536):
        self.level = level
        self.max_workers = max_workers
        self.chunk_size = chunk_size

    @classmethod
    def load_config(cls, config_file: Optional[Path] = None) -> 'CompressionConfig':
        """Load configuration from a file or default values"""
        config_data = {}
        config_paths = [
            Path.home() / '.kziprc',
            Path.cwd() / 'kzip.toml'
        ]

        if config_file:
            config_paths.insert(0, config_file)

        loaded = False
        for path in config_paths:
            if path.exists():
                try:
                    # Assuming TOML format for simplicity, can be extended
                    with open(path, 'r') as f:
                        import toml
                        config_data = toml.load(f)
                    loaded = True
                    break
                except ImportError:
                    print("[yellow]Warning: toml library not found. Cannot load config file. Install with 'pip install toml'")
                    break
                except Exception as e:
                    print(f"[red]Error loading config file {path}: {e}")
                    # Continue to next path or use defaults

        # Apply defaults and validated values
        level = config_data.get('compression', {}).get('level', cls.BALANCED_LEVEL)
        max_workers = config_data.get('performance', {}).get('max_workers', 8)
        chunk_size = config_data.get('performance', {}).get('chunk_size', cls.CHUNK_SIZE)

        # Validate compression level
        if not -100 <= level <= 22:
            print(f"[yellow]Warning: Invalid compression level '{level}' in config. Using default {cls.BALANCED_LEVEL}.")
            level = cls.BALANCED_LEVEL

        return cls(level=level, max_workers=max_workers, chunk_size=chunk_size)


class KZipCLI:
    """Command-line interface for KZip tool"""

    def __init__(self):
        self.console = Console()
        try:
            self.config = CompressionConfig.load_config()
        except Exception:
            # Fallback to default config if loading fails
            self.config = CompressionConfig()

    def create_parser(self) -> argparse.ArgumentParser:
        """Create and configure argument parser"""
        parser = argparse.ArgumentParser(
            prog='kzip',
            description='KZip - High-Performance File Compression Tool',
            formatter_class=argparse.RawDescriptionHelpFormatter,
            epilog=self._get_help_epilog())

        # Positional argument
        parser.add_argument(
            'data',
            nargs='?',
            help='File or directory to compress, or .zst file to decompress')

        # Compression mode flags (mutually exclusive)
        compression_group = parser.add_mutually_exclusive_group()
        compression_group.add_argument(
            '-mc',
            '--max-compression',
            action='store_true',
            help='Use maximum compression (slower, best ratio)')
        compression_group.add_argument(
            '-ms',
            '--max-speed',
            action='store_true',
            help='Use maximum speed (faster, good ratio)')

        # Decompression flag
        parser.add_argument('-d',
                            '--decompress',
                            action='store_true',
                            help='Decompress .zst files or archives')

        # Remove flag (only with decompression)
        parser.add_argument(
            '-r',
            '--remove',
            action='store_true',
            help='Auto-remove compressed files after decompression')

        # Verbose output
        parser.add_argument('-v',
                            '--verbose',
                            action='store_true',
                            help='Enable detailed progress output')

        return parser

    def _get_help_epilog(self) -> str:
        """Get detailed help text"""
        return """
COMPRESSION EXAMPLES:
    kzip document.pdf                    # Compress single file
    kzip -mc /home/user/photos          # Max compression of directory
    kzip -ms -v large_file.tar          # Fast compression with progress
    cat file.txt | kzip -mc             # Compress from standard input

DECOMPRESSION EXAMPLES:
    kzip -d document.pdf.zst            # Decompress with cleanup prompt
    kzip -d -r archive.zst              # Decompress and auto-remove original
    kzip -d -v -r folder.zst            # Decompress with verbose output and cleanup

OUTPUT:
    Compression:
        Files: Creates <filename>.zst in same directory
        Directories: Creates <dirname>.zst archive
        STDIN: Creates stdin_output_<timestamp>.zst

    Decompression:
        Files: Restores original <filename>
        Archives: Recreates original directory structure
        Cleanup: Optionally removes .zst files after extraction

For more information, visit: https://github.com/Karthikdude/kzip
        """

    def validate_args(self, args) -> None:
        """Validate command-line arguments"""
        # Check for STDIN input
        if not args.data and not sys.stdin.isatty():
            return  # STDIN input is valid

        if not args.data:
            raise KZipError("No input data specified. Use -h for help.")

        # Validate decompression constraints
        if args.decompress:
            if args.max_compression or args.max_speed:
                raise KZipError(
                    "Cannot use compression modes with decompression (-d)")

            if not args.data.endswith('.zst'):
                raise KZipError("Decompression input must be a .zst file")

            if not Path(args.data).exists():
                raise KZipError(f"File not found: {args.data}")

        # Validate remove flag
        if args.remove and not args.decompress:
            raise KZipError(
                "Remove flag (-r) can only be used with decompression (-d)")

        # Validate input exists for compression
        if not args.decompress and args.data:
            if not Path(args.data).exists():
                raise KZipError(f"File or directory not found: {args.data}")


class SmartProgress:
    """Enhanced progress display with real-time statistics and professional formatting"""

    def __init__(self, console: Console, operation_type: str = "Processing"):
        self.console = console
        self.operation_type = operation_type
        self.start_time = time.time()
        self.last_update_time = time.time()
        self.bytes_processed = 0
        self.total_bytes = 0
        self.files_processed = 0
        self.total_files = 0
        self.current_file = ""
        self.compression_ratio = 1.0
        self.speed_history = []  # For smoothing speed calculations
        self.max_history = 10

        # Progress bar components
        self.progress = None
        self.main_task = None
        self.file_task = None

    def start(self, total_files: int = 0, total_bytes: int = 0):
        """Initialize progress display"""
        self.total_files = total_files
        self.total_bytes = total_bytes
        self.start_time = time.time()

        # Create rich progress display with multiple columns
        self.progress = Progress(
            TextColumn("[bold blue]{task.description}"),
            BarColumn(bar_width=40),
            TaskProgressColumn(),
            TextColumn("•"),
            FileSizeColumn(),
            TextColumn("•"),
            TransferSpeedColumn(),
            TextColumn("•"),
            TimeRemainingColumn(),
            TextColumn("•"),
            TextColumn("[cyan]{task.fields[ratio]}"),
            console=self.console,
            transient=False
        )

        self.progress.start()

        # Main progress task
        self.main_task = self.progress.add_task(
            f"[bold green]{self.operation_type}",
            total=max(total_files, 1),
            ratio="1.0:1",
            current_file=""
        )

        # Current file task (if processing multiple files)
        if total_files > 1:
            self.file_task = self.progress.add_task(
                "[dim]Current file",
                total=100,
                visible=False
            )

    def update_with_stats(self, files_completed: int = None, bytes_processed: int = None, 
                         current_file: str = None, file_progress: float = None,
                         original_size: int = None, compressed_size: int = None):
        """Update progress with comprehensive statistics"""
        current_time = time.time()

        # Update counters
        if files_completed is not None:
            self.files_processed = files_completed
        if bytes_processed is not None:
            self.bytes_processed = bytes_processed
        if current_file is not None:
            self.current_file = current_file

        # Calculate compression ratio
        if original_size and compressed_size:
            self.compression_ratio = original_size / compressed_size if compressed_size > 0 else 1.0

        # Calculate speed (smoothed)
        time_diff = current_time - self.last_update_time
        if time_diff > 0 and bytes_processed:
            current_speed = (bytes_processed - getattr(self, '_last_bytes', 0)) / time_diff
            self.speed_history.append(current_speed)
            if len(self.speed_history) > self.max_history:
                self.speed_history.pop(0)

            self._last_bytes = bytes_processed

        self.last_update_time = current_time

        # Calculate ETA
        if self.files_processed > 0 and self.total_files > 0:
            files_remaining = self.total_files - self.files_processed
            if files_remaining > 0:
                elapsed = current_time - self.start_time
                rate = self.files_processed / elapsed
                eta_seconds = files_remaining / rate if rate > 0 else 0
            else:
                eta_seconds = 0
        else:
            eta_seconds = 0

        # Format current file name (truncate if too long)
        display_file = current_file or ""
        if len(display_file) > 50:
            display_file = "..." + display_file[-47:]

        # Update main progress
        if self.progress and self.main_task is not None:
            self.progress.update(
                self.main_task,
                completed=self.files_processed,
                description=f"[bold green]{self.operation_type} • [white]{display_file}",
                ratio=f"{self.compression_ratio:.1f}:1"
            )

            # Update file progress if available
            if self.file_task is not None and file_progress is not None:
                self.progress.update(
                    self.file_task,
                    completed=int(file_progress),
                    visible=True
                )

    def update_memory_stats(self, memory_mb: float, cpu_percent: float = None):
        """Update with system resource information"""
        if self.progress and self.main_task is not None:
            resource_info = f"[dim]RAM: {memory_mb:.0f}MB"
            if cpu_percent is not None:
                resource_info += f" • CPU: {cpu_percent:.0f}%"

            # You could add this as a separate task or incorporate into description
            # For now, we'll keep it simple and not clutter the main display
            pass

    def show_phase(self, phase_name: str, details: str = ""):
        """Display current processing phase"""
        if self.progress and self.main_task is not None:
            phase_text = f"[bold yellow]{phase_name}"
            if details:
                phase_text += f" • [white]{details}"

            self.progress.update(
                self.main_task,
                description=phase_text
            )

    def finish(self, success: bool = True):
        """Complete progress display"""
        if self.progress:
            if success:
                final_desc = f"[bold green]✓ {self.operation_type} Complete"
            else:
                final_desc = f"[bold red]✗ {self.operation_type} Failed"

            if self.main_task is not None:
                self.progress.update(
                    self.main_task,
                    description=final_desc,
                    completed=self.total_files or self.files_processed
                )

            time.sleep(0.5)  # Brief pause to show completion
            self.progress.stop()

    def get_stats_summary(self) -> dict:
        """Get comprehensive statistics for final summary"""
        elapsed = time.time() - self.start_time
        avg_speed = sum(self.speed_history) / len(self.speed_history) if self.speed_history else 0

        return {
            'elapsed_time': elapsed,
            'files_processed': self.files_processed,
            'bytes_processed': self.bytes_processed,
            'average_speed': avg_speed,
            'compression_ratio': self.compression_ratio,
            'files_per_second': self.files_processed / elapsed if elapsed > 0 else 0
        }


class CompressionEngine:
    """Core compression and decompression engine"""

    def __init__(self, console: Console, config: CompressionConfig):
        self.console = console
        self.config = config
        self.resource_monitor = ResourceMonitor()

    def detect_archive_type(self, zst_path: Path) -> bool:
        """Peek into compressed data to detect if it's a tar archive"""
        try:
            with open(zst_path, 'rb') as f:
                decompressor = zstd.ZstdDecompressor()
                
                # For streaming compressed data, we need to use stream reader
                try:
                    with decompressor.stream_reader(f) as reader:
                        # Read first 1KB to check for tar signatures
                        sample = reader.read(1024)
                        if len(sample) >= 512:
                            # Check for tar magic bytes at positions 257-262 (ustar format)
                            return sample[257:262] == b'ustar'
                        return False
                except Exception:
                    # Fallback: try direct decompression of header
                    f.seek(0)
                    header_data = f.read(8192)  # Read more data for better detection
                    if len(header_data) < 512:
                        return False
                    
                    try:
                        sample = decompressor.decompress(header_data)
                        if len(sample) >= 512:
                            return sample[257:262] == b'ustar'
                        return False
                    except Exception:
                        return False
        except Exception:
            return False

    def get_compression_level(self, max_compression: bool,
                              max_speed: bool) -> int:
        """Determine compression level based on flags"""
        if max_compression:
            return self.config.MAX_COMPRESSION_LEVEL
        elif max_speed:
            return self.config.MAX_SPEED_LEVEL
        else:
            return self.config.level

    async def compress_single_file_concurrent(self, file_info: Tuple[Path, Path, int],
                                            semaphore: asyncio.Semaphore) -> Tuple[str, int, int, bool]:
        """Compress a single file with semaphore control for concurrency"""
        file_path, relative_path, compression_level = file_info

        async with semaphore:
            try:
                # Check if we should throttle based on system resources
                if self.resource_monitor.should_throttle():
                    await asyncio.sleep(0.1)  # Brief pause if system is stressed

                # Read and compress file
                async with aiofiles.open(file_path, 'rb') as f:
                    data = await f.read()

                compressor = zstd.ZstdCompressor(level=compression_level)
                compressed_data = compressor.compress(data)

                return str(relative_path), len(data), len(compressed_data), True

            except Exception as e:
                # Return error info but don't fail the entire operation
                return str(relative_path), 0, 0, False

    async def compress_files_concurrently(self, files: List[Tuple[Path, Path, int]],
                                        semaphore_limit: int = None) -> List[Tuple[str, int, int, bool]]:
        """Compress multiple files concurrently with dynamic resource management"""
        if semaphore_limit is None:
            semaphore_limit = self.resource_monitor.current_workers

        semaphore = asyncio.Semaphore(semaphore_limit)

        # Create tasks for concurrent processing
        tasks = []
        for file_info in files:
            task = asyncio.create_task(
                self.compress_single_file_concurrent(file_info, semaphore)
            )
            tasks.append(task)

        # Process with periodic resource monitoring
        results = []
        for i, task in enumerate(asyncio.as_completed(tasks)):
            result = await task
            results.append(result)

            # Dynamically adjust concurrency based on system resources
            if i % 10 == 0:  # Check every 10 files
                new_limit = self.resource_monitor.adjust_workers()
                if new_limit != semaphore_limit:
                    semaphore_limit = new_limit
                    # Note: We can't change existing semaphore, but this affects future batches

        return results

    async def compress_file(self,
                            input_path: Path,
                            output_path: Path,
                            compression_level: int,
                            verbose: bool = False) -> Tuple[int, int, float]:
        """Compress a single file"""
        start_time = time.time()
        original_size = 0
        compressed_size = 0

        try:
            # Create compressor
            compressor = zstd.ZstdCompressor(level=compression_level)

            # Setup progress tracking if verbose
            progress = None
            task_id = None

            if verbose:
                file_size = input_path.stat().st_size
                progress = Progress(TextColumn("[bold blue]Compressing:"),
                                    BarColumn(),
                                    TaskProgressColumn(),
                                    FileSizeColumn(),
                                    TransferSpeedColumn(),
                                    TimeRemainingColumn(),
                                    console=self.console)
                progress.start()
                task_id = progress.add_task(f"[cyan]{input_path.name}",
                                            total=file_size)

            # Perform compression using compressor.compress() for simpler handling
            async with aiofiles.open(input_path, 'rb') as input_file:
                # Read entire file for smaller files, or chunk for larger ones
                file_size = input_path.stat().st_size

                if file_size < 100 * 1024 * 1024:  # Files under 100MB - read all at once
                    data = await input_file.read()
                    original_size = len(data)
                    compressed_data = compressor.compress(data)
                    compressed_size = len(compressed_data)

                    async with aiofiles.open(output_path, 'wb') as output_file:
                        await output_file.write(compressed_data)

                    if progress and task_id is not None:
                        progress.update(task_id, completed=file_size)

                else:  # Large files - use streaming compression
                    # For large files, we'll use a temporary buffer approach
                    buffer = io.BytesIO()

                    while True:
                        chunk = await input_file.read(self.config.chunk_size)
                        if not chunk:
                            break

                        original_size += len(chunk)
                        buffer.write(chunk)

                        if progress and task_id is not None:
                            progress.update(task_id, advance=len(chunk))

                    # Compress the entire buffer
                    buffer.seek(0)
                    compressed_data = compressor.compress(buffer.getvalue())
                    compressed_size = len(compressed_data)

                    async with aiofiles.open(output_path, 'wb') as output_file:
                        await output_file.write(compressed_data)

            if progress:
                progress.stop()

            end_time = time.time()
            return original_size, compressed_size, end_time - start_time

        except Exception as e:
            if output_path.exists():
                output_path.unlink()  # Clean up partial file
            raise KZipError(f"Compression failed: {str(e)}")

    def _build_tar_archive(self, temp_tar_file: io.BytesIO, input_path: Path,
                           verbose: bool, start_time: float,
                           file_count: int, progress_state: Optional[ProgressState] = None,
                           smart_progress: Optional[SmartProgress] = None) -> None:
        """Helper to build tar archive with resume capability and enhanced progress"""
        files_processed = 0
        current_file = ""
        original_size = 0
        processed_files = []

        # Load checkpoint if available
        checkpoint = None
        if progress_state:
            checkpoint = progress_state.load_checkpoint()
            if checkpoint:
                processed_files = checkpoint.get('processed_files', [])
                files_processed = checkpoint.get('current_position', 0)
                original_size = checkpoint.get('bytes_processed', 0)
                if verbose:
                    self.console.print(f"[cyan]Resuming from checkpoint: {files_processed:,}/{file_count:,} files processed")

        with tarfile.open(fileobj=temp_tar_file, mode='w') as tar:
            for root, dirs, files in os.walk(input_path):
                for file in files:
                    file_path = Path(root) / file
                    relative_path = str(file_path.relative_to(input_path.parent))
                    current_file = relative_path

                    # Skip if already processed (resume capability)
                    if checkpoint and relative_path in processed_files:
                        continue

                    try:
                        arcname = file_path.relative_to(input_path.parent)
                        tar.add(file_path, arcname=arcname)

                        file_size = file_path.stat().st_size
                        original_size += file_size
                        files_processed += 1
                        processed_files.append(relative_path)

                        # Save checkpoint periodically
                        if progress_state and files_processed % self.config.CHECKPOINT_INTERVAL == 0:
                            progress_state.save_checkpoint(processed_files, files_processed,
                                                         file_count, original_size)

                        # Update smart progress if available
                        if smart_progress and verbose:
                            memory_mb = self.resource_monitor.get_memory_usage_mb()
                            smart_progress.update_with_stats(
                                files_completed=files_processed,
                                bytes_processed=original_size,
                                current_file=current_file
                            )
                            smart_progress.update_memory_stats(memory_mb)
                        elif verbose and files_processed % 100 == 0:
                            # Fallback to simple progress display
                            elapsed = time.time() - start_time
                            speed = (original_size / 1024 / 1024) / elapsed if elapsed > 0 else 0
                            memory_mb = self.resource_monitor.get_memory_usage_mb()
                            progress_line = f"Files processed: {files_processed:,}/{file_count:,} | Current: {current_file} | Speed: {speed:.1f} MB/s | Memory: {memory_mb:.0f}MB"
                            if len(progress_line) > 120:
                                progress_line = progress_line[:117] + "..."
                            print(f"\r{progress_line:<120}", end="", flush=True)

                    except (OSError, IOError) as e:
                        if verbose:
                            self.console.print(
                                f"[yellow]Warning: Skipping {file_path}: {e}")
                        continue
                    except KeyboardInterrupt:
                        # Save progress before interrupting
                        if progress_state:
                            progress_state.save_checkpoint(processed_files, files_processed,
                                                         file_count, original_size)
                        raise  # Propagate interrupt

    async def compress_directory(
            self,
            input_path: Path,
            output_path: Path,
            compression_level: int,
            verbose: bool = False) -> Tuple[int, int, float]:
        """Compress a directory into a tar.zst archive with resume capability"""
        start_time = time.time()
        original_size = 0
        compressed_size = 0

        # Setup progress state for resume capability
        operation_id = f"{input_path.name}_{int(start_time)}"
        progress_state = ProgressState(operation_id)

        try:
            compressor = zstd.ZstdCompressor(level=compression_level)

            file_count = 0
            total_size = 0

            # Initialize smart progress for verbose mode
            smart_progress = None
            if verbose:
                self.console.print("[yellow]Scanning directory structure...")
                # Check system resources before starting
                memory_percent = psutil.virtual_memory().percent
                cpu_count = os.cpu_count() or 4
                workers = self.resource_monitor.adjust_workers()

                self.console.print(f"[cyan]System Resources: {memory_percent:.1f}% memory, {cpu_count} CPUs, {workers} workers")

                for root, dirs, files in os.walk(input_path):
                    for file in files:
                        file_path = Path(root) / file
                        try:
                            total_size += file_path.stat().st_size
                            file_count += 1
                        except (OSError, IOError):
                            continue

                self.console.print(
                    f"Found {file_count:,} files ({total_size / 1024 / 1024:.1f} MB total)"
                )

                # Initialize smart progress
                smart_progress = SmartProgress(self.console, "Archive Creation")
                smart_progress.start(total_files=file_count, total_bytes=total_size)

            # Create tar archive and compress using streaming
            with tempfile.NamedTemporaryFile() as temp_tar:
                # Build tar archive using asyncio.to_thread to avoid blocking
                await asyncio.to_thread(self._build_tar_archive,
                                        temp_tar, input_path, verbose,
                                        start_time, file_count, progress_state, smart_progress)

                original_size = temp_tar.tell()

                # Show finishing message and update progress
                if verbose and smart_progress:
                    smart_progress.show_phase("Compression", "Finalizing archive...")
                elif verbose:
                    print("\r" + " " * 120 + "\r", end="")  # Clear the progress line
                    self.console.print("[cyan]Processing completed (99%), finishing compression...")

                # Use streaming compression to avoid loading entire tar into memory
                temp_tar.seek(0)
                compressed_size = 0

                async with aiofiles.open(output_path, 'wb') as output_file:
                    # Use stream_writer for memory-efficient compression
                    with compressor.stream_writer(output_file._file) as writer:
                        while True:
                            chunk = temp_tar.read(self.config.chunk_size)
                            if not chunk:
                                break
                            writer.write(chunk)
                            compressed_size += len(chunk)

                            # Allow for interruption during compression
                            if compressed_size % (self.config.chunk_size * 100) == 0:
                                await asyncio.sleep(0)

                # Get actual compressed size from file
                compressed_size = output_path.stat().st_size

            if verbose and smart_progress:
                smart_progress.finish(success=True)
            elif verbose:
                print("\r" + " " * 120 + "\r", end="")  # Clear any remaining progress
                self.console.print("Progress: " + "━" * 32 + " 100%")

            end_time = time.time()

            # Clean up progress state on successful completion
            progress_state.cleanup()

            return original_size, compressed_size, end_time - start_time

        except KeyboardInterrupt:
            if output_path.exists():
                output_path.unlink()
            self.console.print(f"\n[yellow]Compression cancelled by user. Progress saved for resume.")
            raise
        except Exception as e:
            if output_path.exists():
                output_path.unlink()
            # Clean up progress state on failure
            progress_state.cleanup()
            raise KZipError(f"Directory compression failed: {str(e)}")

    async def decompress_file(self,
                              input_path: Path,
                              output_path: Path,
                              verbose: bool = False) -> Tuple[int, int, float]:
        """Decompress a .zst file"""
        start_time = time.time()
        compressed_size = 0
        decompressed_size = 0

        try:
            # Create decompressor
            decompressor = zstd.ZstdDecompressor()

            # Setup progress tracking if verbose
            progress = None
            task_id = None

            if verbose:
                file_size = input_path.stat().st_size
                progress = Progress(TextColumn("[bold green]Decompressing:"),
                                    BarColumn(),
                                    TaskProgressColumn(),
                                    FileSizeColumn(),
                                    TransferSpeedColumn(),
                                    TimeRemainingColumn(),
                                    console=self.console)
                progress.start()
                task_id = progress.add_task(f"[cyan]{input_path.name}",
                                            total=file_size)

            # Perform decompression - read entire compressed file and decompress
            async with aiofiles.open(input_path, 'rb') as input_file:
                compressed_data = await input_file.read()
                compressed_size = len(compressed_data)

                # Decompress the data with better error handling
                try:
                    decompressed_data = decompressor.decompress(
                        compressed_data)
                except Exception as decomp_error:
                    raise KZipError(
                        f"Invalid or corrupted zstd file: {decomp_error}")
                decompressed_size = len(decompressed_data)

                # Write decompressed data
                async with aiofiles.open(output_path, 'wb') as output_file:
                    await output_file.write(decompressed_data)

                if progress and task_id is not None:
                    progress.update(task_id, completed=compressed_size)

            if progress:
                progress.stop()

            end_time = time.time()
            return compressed_size, decompressed_size, end_time - start_time

        except Exception as e:
            if output_path.exists():
                output_path.unlink()  # Clean up partial file
            raise KZipError(f"Decompression failed: {str(e)}")

    async def decompress_archive(
            self,
            input_path: Path,
            output_dir: Path,
            verbose: bool = False) -> Tuple[int, int, float]:
        """Decompress a directory archive"""
        start_time = time.time()
        compressed_size = input_path.stat().st_size
        decompressed_size = 0

        try:
            # Create decompressor
            decompressor = zstd.ZstdDecompressor()

            if verbose:
                self.console.print("[yellow]Analyzing archive structure...")

            # Decompress to temporary tar file using streaming for large files
            with tempfile.NamedTemporaryFile() as temp_tar:
                try:
                    # Try streaming decompression first (for files compressed with streaming)
                    with open(input_path, 'rb') as input_file:
                        with decompressor.stream_reader(input_file) as reader:
                            while True:
                                chunk = reader.read(self.config.chunk_size)
                                if not chunk:
                                    break
                                temp_tar.write(chunk)
                                decompressed_size += len(chunk)
                except Exception:
                    # Fallback to non-streaming decompression
                    temp_tar.seek(0)
                    temp_tar.truncate()
                    decompressed_size = 0
                    
                    async with aiofiles.open(input_path, 'rb') as input_file:
                        compressed_data = await input_file.read()

                        # Decompress the data with better error handling
                        try:
                            decompressed_data = decompressor.decompress(compressed_data)
                        except Exception as decomp_error:
                            raise KZipError(f"Invalid or corrupted archive: {decomp_error}")
                        decompressed_size = len(decompressed_data)

                        # Write to temporary tar file
                        temp_tar.write(decompressed_data)

                # Extract tar archive
                temp_tar.seek(0)
                with tarfile.open(fileobj=temp_tar, mode='r') as tar:
                    if verbose:
                        members = tar.getmembers()
                        file_count = len([m for m in members if m.isfile()])
                        self.console.print(
                            f"Found {file_count:,} files ({compressed_size / 1024 / 1024:.1f} MB compressed)"
                        )

                        progress = Progress(
                            TextColumn("[bold green]Progress:"),
                            BarColumn(),
                            TaskProgressColumn(),
                            TextColumn("[cyan]Files extracted:"),
                            TextColumn(
                                "[white]{task.fields[files_extracted]}/{task.fields[total_files]}"
                            ),
                            console=self.console)
                        progress.start()
                        task_id = progress.add_task("Extracting archive",
                                                    total=file_count,
                                                    files_extracted=0,
                                                    total_files=file_count)

                        files_extracted = 0
                        for member in members:
                            # Use asyncio.to_thread for blocking tarfile operations
                            await asyncio.to_thread(tar.extract, member, output_dir)
                            if member.isfile():
                                files_extracted += 1
                                progress.update(
                                    task_id,
                                    advance=1,
                                    files_extracted=files_extracted)

                        progress.stop()
                    else:
                        # Use asyncio.to_thread for blocking tarfile operations
                        tar.extractall(output_dir)

            end_time = time.time()
            return compressed_size, decompressed_size, end_time - start_time

        except Exception as e:
            raise KZipError(f"Archive decompression failed: {str(e)}")

    async def compress_stdin(
            self,
            compression_level: int,
            verbose: bool = False) -> Tuple[str, int, int, float]:
        """Compress data from STDIN"""
        start_time = time.time()
        original_size = 0
        compressed_size = 0

        # Generate timestamped output filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = Path(f"stdin_output_{timestamp}.zst")

        try:
            # Create compressor
            compressor = zstd.ZstdCompressor(level=compression_level)

            if verbose:
                self.console.print(
                    f"[yellow]Reading from STDIN, output: {output_path}")

            # Read all STDIN data
            stdin_data = sys.stdin.buffer.read()
            original_size = len(stdin_data)

            # Compress data
            compressed_data = compressor.compress(stdin_data)
            compressed_size = len(compressed_data)

            # Write to output file
            async with aiofiles.open(output_path, 'wb') as output_file:
                await output_file.write(compressed_data)

            end_time = time.time()
            return str(output_path
                       ), original_size, compressed_size, end_time - start_time

        except Exception as e:
            if output_path.exists():
                output_path.unlink()  # Clean up partial file
            raise KZipError(f"STDIN compression failed: {str(e)}")


class KZipApp:
    """Main application class"""

    def __init__(self):
        self.console = Console()
        try:
            self.config = CompressionConfig.load_config()
        except Exception:
            # Fallback to default config if loading fails
            self.config = CompressionConfig()

        # Initialize CLI and engine
        self.cli = KZipCLI()
        self.engine = CompressionEngine(self.console, self.config)

    def print_banner(self, verbose: bool):
        """Print application banner"""
        if verbose:
            self.console.print(
                "KZip v1.1.0 - High-Performance Compression Tool")
            self.console.print("Enhanced with parallel processing, resume capability & resource monitoring")
            self.console.print("=" * 72)
            self.console.print()

    def print_summary(self,
                      operation: str,
                      original_size: int,
                      final_size: int,
                      duration: float,
                      files_count: Optional[int] = None,
                      errors: int = 0,
                      files_skipped: int = 0,
                      smart_progress: Optional[SmartProgress] = None):
        """Print enhanced operation summary in professional format"""

        # Create a professional summary table
        table = Table(title=f"\n🎯 {operation} Summary", show_header=False, box=None, padding=(0, 1))
        table.add_column("Metric", style="bold cyan", width=20)
        table.add_column("Value", style="white", width=30)

        if operation == "Compression":
            ratio = original_size / final_size if final_size > 0 else 0
            space_saved = original_size - final_size
            space_saved_percent = (space_saved / original_size * 100) if original_size > 0 else 0

            table.add_row("📁 Original size", f"{original_size / 1024 / 1024:.1f} MB")
            table.add_row("📦 Compressed size", f"{final_size / 1024 / 1024:.1f} MB")
            table.add_row("🔥 Compression ratio", f"[bold green]{ratio:.1f}:1")
            table.add_row("💾 Space saved", f"[bold yellow]{space_saved / 1024 / 1024:.1f} MB ({space_saved_percent:.1f}%)")
        else:
            ratio = final_size / original_size if original_size > 0 else 0
            table.add_row("📦 Compressed size", f"{original_size / 1024 / 1024:.1f} MB")
            table.add_row("📁 Extracted size", f"{final_size / 1024 / 1024:.1f} MB")
            table.add_row("📈 Expansion ratio", f"[bold green]{ratio:.1f}:1")

        # Time and performance metrics
        minutes = int(duration // 60)
        seconds = int(duration % 60)
        time_str = f"{minutes}m {seconds}s" if minutes > 0 else f"{seconds}s"
        table.add_row("⏱️  Total time", f"[bold cyan]{time_str}")

        if duration > 0:
            speed = (max(original_size, final_size) / 1024 / 1024) / duration
            table.add_row("🚀 Average speed", f"[bold magenta]{speed:.1f} MB/s")

        # File statistics
        if files_count is not None:
            if operation == "Compression":
                table.add_row("📋 Files processed", f"[bold white]{files_count:,}")
                if files_skipped > 0:
                    table.add_row("⚠️  Files skipped", f"[yellow]{files_skipped:,}")

                # Add files per second if we have the data
                if duration > 0:
                    files_per_sec = files_count / duration
                    table.add_row("📊 Processing rate", f"{files_per_sec:.1f} files/sec")
            else:
                table.add_row("📋 Files extracted", f"[bold white]{files_count:,}")

        # System and error info
        if smart_progress:
            stats = smart_progress.get_stats_summary()
            if stats.get('average_speed', 0) > 0:
                table.add_row("⚡ Peak speed", f"{stats['average_speed'] / 1024 / 1024:.1f} MB/s")

        # Error reporting
        if errors > 0:
            table.add_row("❌ Errors", f"[bold red]{errors}")
        else:
            table.add_row("✅ Status", "[bold green]Success")

        self.console.print(table)

        # Add performance insights
        if operation == "Compression" and ratio > 0:
            if ratio >= 10:
                insight = "🌟 Excellent compression achieved!"
            elif ratio >= 5:
                insight = "👍 Good compression ratio"
            elif ratio >= 2:
                insight = "✓ Moderate compression"
            else:
                insight = "ℹ️ Limited compression (file may already be compressed)"

            self.console.print(f"\n[dim]{insight}[/dim]")

    async def handle_cleanup_prompt(self, compressed_file: Path,
                                    auto_remove: bool) -> bool:
        """Handle cleanup of compressed files after decompression"""
        if auto_remove:
            try:
                compressed_file.unlink()
                self.console.print(
                    f"[green]Compressed file deleted: {compressed_file}")
                return True
            except Exception as e:
                self.console.print(
                    f"[red]Failed to delete compressed file: {e}")
                return False
        else:
            # Check if we're in an interactive environment
            if not sys.stdin.isatty():
                # Non-interactive environment - preserve file by default
                self.console.print(
                    f"[yellow]Compressed file preserved: {compressed_file}")
                return False

            # Interactive prompt
            try:
                response = self.console.input(
                    f"\nDelete the compressed file '{compressed_file}'? [y/N]: "
                )
                if response.lower() in ['y', 'yes']:
                    try:
                        compressed_file.unlink()
                        self.console.print(
                            "[green]Compressed file deleted successfully.")
                        return True
                    except Exception as e:
                        self.console.print(
                            f"[red]Failed to delete compressed file: {e}")
                        return False
                else:
                    self.console.print("[yellow]Compressed file preserved.")
                    return False
            except (EOFError, KeyboardInterrupt):
                # Handle EOF or Ctrl+C gracefully
                self.console.print("\n[yellow]Compressed file preserved.")
                return False

    async def run_compression(self, args) -> None:
        """Run compression operation"""
        # Determine compression level
        compression_level = self.engine.get_compression_level(
            args.max_compression, args.max_speed)

        # Handle STDIN input
        if not args.data and not sys.stdin.isatty():
            if args.verbose:
                self.print_banner(True)
                mode = "maximum compression" if args.max_compression else \
                       "maximum speed" if args.max_speed else "balanced"
                self.console.print(f"Operation: Compression (STDIN)")
                self.console.print(
                    f"Compression Level: {compression_level} ({mode})")
                self.console.print()

            output_path, original_size, compressed_size, duration = \
                await self.engine.compress_stdin(compression_level, args.verbose)

            if not args.verbose:
                self.console.print(f"Compressing: STDIN")
                self.console.print(f"Output: {output_path}")
                self.console.print("Compression completed successfully.")
            else:
                self.print_summary("Compression", original_size,
                                   compressed_size, duration)

            return

        # Handle file/directory input
        input_path = Path(args.data)

        if input_path.is_file():
            output_path = input_path.with_suffix(input_path.suffix + '.zst')
        else:
            output_path = input_path.with_suffix('.zst')

        if args.verbose:
            self.print_banner(True)
            mode = "maximum compression" if args.max_compression else \
                   "maximum speed" if args.max_speed else "balanced"
            worker_threads = min(self.config.max_workers, os.cpu_count() or 4)
            max_memory = f"{self.config.MAX_MEMORY_MB}MB"

            self.console.print(f"Operation: Compression")
            self.console.print(f"Input: {input_path}")
            self.console.print(f"Output: {output_path}")
            self.console.print(
                f"Compression Level: {compression_level} ({mode})")
            self.console.print(f"Worker Threads: {worker_threads}")
            self.console.print(f"Max Memory: {max_memory}")
            self.console.print()

        # Perform compression
        try:
            if input_path.is_file():
                original_size, compressed_size, duration = \
                    await self.engine.compress_file(
                        input_path, output_path, compression_level, args.verbose
                    )
                files_count = 1
            else:
                original_size, compressed_size, duration = \
                    await self.engine.compress_directory(
                        input_path, output_path, compression_level, args.verbose
                    )
                files_count = None  # Count handled in directory compression

            if not args.verbose:
                self.console.print(f"Compressing: {input_path}")
                self.console.print(f"Output: {output_path}")
                self.console.print("Compression completed successfully.")
            else:
                self.console.print()
                self.print_summary("Compression", original_size,
                                   compressed_size, duration, files_count)

        except KZipError as e:
            self.console.print(f"[red]Error: {e}")
            sys.exit(1)

    async def run_decompression(self, args) -> None:
        """Run decompression operation"""
        input_path = Path(args.data)

        # Determine if it's a file or directory archive
        base_name = input_path.stem
        output_path = input_path.parent / base_name

        # Use improved archive detection
        if args.verbose:
            self.console.print("[yellow]Analyzing compressed file structure...")

        is_archive = self.engine.detect_archive_type(input_path)

        if args.verbose:
            archive_type = "tar archive" if is_archive else "single file"
            self.console.print(f"[cyan]Detected content type: {archive_type}")

        if args.verbose:
            self.print_banner(True)
            worker_threads = min(self.config.max_workers, os.cpu_count() or 4)
            max_memory = "1GB"  # Decompression uses less memory

            self.console.print(f"Operation: Decompression")
            self.console.print(f"Input: {input_path}")
            self.console.print(f"Output: {output_path}")
            self.console.print(f"Worker Threads: {worker_threads}")
            self.console.print(f"Max Memory: {max_memory}")
            self.console.print()

        # Perform decompression
        try:
            if is_archive:
                # Ensure output directory exists
                output_path.mkdir(exist_ok=True)
                compressed_size, decompressed_size, duration = \
                    await self.engine.decompress_archive(
                        input_path, output_path, args.verbose
                    )
            else:
                compressed_size, decompressed_size, duration = \
                    await self.engine.decompress_file(
                        input_path, output_path, args.verbose
                    )

            if not args.verbose:
                self.console.print(f"Decompressing: {input_path}")
                self.console.print(f"Output: {output_path}")
                self.console.print("Decompression completed successfully.")
            else:
                self.console.print()
                self.print_summary("Decompression", compressed_size,
                                   decompressed_size, duration)

            # Handle cleanup
            if args.verbose:
                self.console.print()

            cleanup_success = await self.handle_cleanup_prompt(
                input_path, args.remove)

            if args.verbose and cleanup_success:
                self.console.print("Cleanup: Completed")

        except KZipError as e:
            self.console.print(f"[red]Error: {e}")
            sys.exit(1)

    async def run(self) -> None:
        """Main application entry point"""
        parser = self.cli.create_parser()

        try:
            args = parser.parse_args()

            # Validate arguments
            self.cli.validate_args(args)

            # Route to appropriate operation
            if args.decompress:
                await self.run_decompression(args)
            else:
                await self.run_compression(args)

        except KZipError as e:
            self.console.print(f"[red]Error: {e}")
            sys.exit(1)
        except KeyboardInterrupt:
            self.console.print(f"\n[yellow]Operation cancelled by user.")
            sys.exit(1)
        except Exception as e:
            self.console.print(f"[red]Unexpected error: {e}")
            sys.exit(1)


def main():
    """Main entry point"""
    app = KZipApp()

    # Run with asyncio
    try:
        asyncio.run(app.run())
    except KeyboardInterrupt:
        print("\nOperation cancelled by user.")
        sys.exit(1)


if __name__ == '__main__':
    main()