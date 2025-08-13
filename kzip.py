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
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple, Union
import io

try:
    import zstandard as zstd
    import aiofiles
    from rich.console import Console
    from rich.progress import (Progress, TextColumn, BarColumn,
                               TaskProgressColumn, TimeRemainingColumn,
                               FileSizeColumn, TransferSpeedColumn)
    from rich.table import Table
    from rich.panel import Panel
except ImportError as e:
    print(f"Required dependency missing: {e}")
    print("Please install required packages:")
    print("pip install zstandard aiofiles rich")
    sys.exit(1)


class KZipError(Exception):
    """Base exception for KZip operations"""
    pass


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


class KZipCLI:
    """Command-line interface for KZip tool"""

    def __init__(self):
        self.console = Console()
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

For more information, visit: https://github.com/yourusername/kzip
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


class CompressionEngine:
    """Core compression and decompression engine"""

    def __init__(self, console: Console, config: CompressionConfig):
        self.console = console
        self.config = config

    def get_compression_level(self, max_compression: bool,
                              max_speed: bool) -> int:
        """Determine compression level based on flags"""
        if max_compression:
            return self.config.MAX_COMPRESSION_LEVEL
        elif max_speed:
            return self.config.MAX_SPEED_LEVEL
        else:
            return self.config.BALANCED_LEVEL

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
                        chunk = await input_file.read(self.config.CHUNK_SIZE)
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

    async def compress_directory(
            self,
            input_path: Path,
            output_path: Path,
            compression_level: int,
            verbose: bool = False) -> Tuple[int, int, float]:
        """Compress a directory into a tar.zst archive"""
        start_time = time.time()
        original_size = 0
        compressed_size = 0

        try:
            # Create compressor
            compressor = zstd.ZstdCompressor(level=compression_level)

            # Count files and calculate total size for progress
            file_count = 0
            total_size = 0

            if verbose:
                self.console.print("[yellow]Scanning directory structure...")
                for root, dirs, files in os.walk(input_path):
                    for file in files:
                        file_path = Path(root) / file
                        try:
                            total_size += file_path.stat().st_size
                            file_count += 1
                        except (OSError, IOError):
                            continue  # Skip inaccessible files

                self.console.print(
                    f"Found {file_count:,} files ({total_size / 1024 / 1024:.1f} MB total)"
                )

            # Setup progress tracking
            progress = None
            task_id = None

            if verbose:
                # We'll use custom single-line progress instead of Rich progress
                progress = None
                task_id = None

            # Create tar archive first, then compress
            with tempfile.NamedTemporaryFile() as temp_tar:
                with tarfile.open(fileobj=temp_tar, mode='w') as tar:
                    files_processed = 0
                    current_file = ""

                    for root, dirs, files in os.walk(input_path):
                        for file in files:
                            file_path = Path(root) / file
                            current_file = str(
                                file_path.relative_to(input_path.parent))

                            try:
                                # Add file to tar with relative path
                                arcname = file_path.relative_to(
                                    input_path.parent)
                                tar.add(file_path, arcname=arcname)

                                file_size = file_path.stat().st_size
                                original_size += file_size
                                files_processed += 1

                                # Update status line every 100 files (single line, edit in place)
                                if verbose and files_processed % 100 == 0:
                                    elapsed = time.time() - start_time
                                    speed = (original_size / 1024 / 1024
                                             ) / elapsed if elapsed > 0 else 0
                                    # Clear line and update in place using \r and terminal control
                                    progress_line = f"Files processed: {files_processed:,}/{file_count:,} | Current: {current_file} | Speed: {speed:.1f} MB/s"
                                    # Truncate if too long and ensure it overwrites completely
                                    if len(progress_line) > 120:
                                        progress_line = progress_line[:117] + "..."
                                    print(f"\r{progress_line:<120}",
                                          end="",
                                          flush=True)

                                # Check for cancellation periodically
                                if files_processed % 100 == 0:
                                    await asyncio.sleep(
                                        0)  # Allow for interruption

                            except (OSError, IOError) as e:
                                if verbose:
                                    self.console.print(
                                        f"[yellow]Warning: Skipping {file_path}: {e}"
                                    )
                                continue
                            except KeyboardInterrupt:
                                if progress:
                                    progress.stop()
                                raise

                # Show finishing message at 99% before final compression step
                if verbose:
                    print("\r" + " " * 120 + "\r",
                          end="")  # Clear the progress line completely
                    self.console.print(
                        "[cyan]Processing completed (99%), finishing compression..."
                    )

                # Now compress the entire tar file
                temp_tar.seek(0)
                tar_data = temp_tar.read()

                # Compress using the simple compress method (handles framing properly)
                compressed_data = compressor.compress(tar_data)
                compressed_size = len(compressed_data)

                # Write compressed data
                async with aiofiles.open(output_path, 'wb') as output_file:
                    await output_file.write(compressed_data)

            # Create simple progress bar manually for completion
            if verbose:
                print("\r" + " " * 120 + "\r",
                      end="")  # Clear any remaining progress
                self.console.print("Progress: " + "━" * 32 + " 100%")

            end_time = time.time()
            return original_size, compressed_size, end_time - start_time

        except KeyboardInterrupt:
            # Handle Ctrl+C gracefully
            if 'progress' in locals() and progress:
                progress.stop()
            if output_path.exists():
                output_path.unlink()  # Clean up partial file
            self.console.print("\n[yellow]Compression cancelled by user.")
            raise
        except Exception as e:
            if 'progress' in locals() and progress:
                progress.stop()
            if output_path.exists():
                output_path.unlink()  # Clean up partial file
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

            # Decompress to temporary tar file first
            with tempfile.NamedTemporaryFile() as temp_tar:
                async with aiofiles.open(input_path, 'rb') as input_file:
                    compressed_data = await input_file.read()

                    # Decompress the data with better error handling
                    try:
                        decompressed_data = decompressor.decompress(
                            compressed_data)
                    except Exception as decomp_error:
                        raise KZipError(
                            f"Invalid or corrupted archive: {decomp_error}")
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
                            tar.extract(member, output_dir)
                            if member.isfile():
                                files_extracted += 1
                                progress.update(
                                    task_id,
                                    advance=1,
                                    files_extracted=files_extracted)

                        progress.stop()
                    else:
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
        self.config = CompressionConfig()
        self.engine = CompressionEngine(self.console, self.config)
        self.cli = KZipCLI()

    def print_banner(self, verbose: bool):
        """Print application banner"""
        if verbose:
            self.console.print(
                "KZip v1.0.0 - High-Performance Compression Tool")
            self.console.print("=" * 40)
            self.console.print()

    def print_summary(self,
                      operation: str,
                      original_size: int,
                      final_size: int,
                      duration: float,
                      files_count: Optional[int] = None,
                      errors: int = 0,
                      files_skipped: int = 0):
        """Print operation summary in professional format"""
        self.console.print(f"\n{operation} Summary:")

        if operation == "Compression":
            ratio = original_size / final_size if final_size > 0 else 0
            self.console.print(
                f"- Original size: {original_size / 1024 / 1024:.1f} MB")
            self.console.print(
                f"- Compressed size: {final_size / 1024 / 1024:.1f} MB")
            self.console.print(f"- Compression ratio: {ratio:.1f}:1")
        else:
            ratio = final_size / original_size if original_size > 0 else 0
            self.console.print(
                f"- Compressed size: {original_size / 1024 / 1024:.1f} MB")
            self.console.print(
                f"- Extracted size: {final_size / 1024 / 1024:.1f} MB")
            self.console.print(f"- Expansion ratio: {ratio:.1f}:1")

        minutes = int(duration // 60)
        seconds = int(duration % 60)
        self.console.print(f"- Total time: {minutes}m {seconds}s")

        if duration > 0:
            speed = (max(original_size, final_size) / 1024 / 1024) / duration
            self.console.print(f"- Average speed: {speed:.1f} MB/s")

        if files_count is not None:
            if operation == "Compression":
                self.console.print(f"- Files processed: {files_count:,}")
                if files_skipped > 0:
                    self.console.print(f"- Files skipped: {files_skipped:,}")
            else:
                self.console.print(f"- Files extracted: {files_count:,}")

        self.console.print(f"- Errors: {errors}")

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
            worker_threads = min(8,
                                 os.cpu_count()
                                 or 4)  # Simulate worker threads
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

        # Determine archive type using simple but reliable heuristics
        is_archive = False

        # Simple and reliable heuristics:
        if 'stdin_output_' in base_name:
            # STDIN outputs are always single files
            is_archive = False
        elif output_path.exists() and output_path.is_dir():
            # If output directory already exists, it's likely an archive
            is_archive = True
        elif '.' in base_name:
            # If base name has an extension, it's likely a single file
            is_archive = False
        else:
            # No extension suggests it was a directory
            is_archive = True

        if args.verbose:
            self.print_banner(True)
            worker_threads = min(8,
                                 os.cpu_count()
                                 or 4)  # Simulate worker threads
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
