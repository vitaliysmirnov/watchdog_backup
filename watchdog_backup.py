import os
import time
import ctypes
import logging
import logging.handlers
import subprocess
import platform
import shutil
import schedule
from datetime import datetime

# Constants
CONFIG_FILE = "config.txt"
LOG_FILE = "watchdog_backup.log"
ROBOCOPY_LOG = "robocopy.log"
MAX_ROBOCOPY_LOG_SIZE = 10 * 1024 * 1024  # 10 MB
MAX_LOG_FILES = 10
LAST_COPY_FILE = ".last_copy_time"


def setup_logging():
    """Configure logging system"""
    logger = logging.getLogger('BackupLogger')
    logger.setLevel(logging.INFO)

    # Main log file with rotation
    handler = logging.handlers.TimedRotatingFileHandler(
        LOG_FILE, when='midnight', backupCount=MAX_LOG_FILES
    )
    handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(handler)

    # Console output
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter('%(message)s'))
    logger.addHandler(console_handler)

    return logger


def is_robocopy_available():
    """Check Robocopy availability with result caching"""
    if not hasattr(is_robocopy_available, 'available'):
        try:
            subprocess.run(["robocopy", "/?"],
                           stdout=subprocess.DEVNULL,
                           stderr=subprocess.DEVNULL)
            is_robocopy_available.available = True
        except:
            is_robocopy_available.available = False
    return is_robocopy_available.available


def is_disk_connected(disk_name):
    """Check if disk is connected"""
    try:
        if platform.system() == "Windows":
            import win32api
            drives = [f"{d}:\\" for d in "ABCDEFGHIJKLMNOPQRSTUVWXYZ" if os.path.exists(f"{d}:\\")]
            for drive in drives:
                try:
                    vol_name = win32api.GetVolumeInformation(drive)[0]
                    if disk_name.upper() in vol_name.upper():
                        return drive
                except:
                    continue
        else:
            # For Linux/Mac
            result = subprocess.run(["lsblk", "-o", "LABEL,MOUNTPOINT", "-n"],
                                    stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                    text=True, check=True)
            for line in result.stdout.splitlines():
                if disk_name in line:
                    return line.split()[-1]
    except Exception as e:
        logging.error(f"Disk check error: {str(e)}")
    return None


def check_rsync_available():
    """Check rsync availability with result caching"""
    if not hasattr(check_rsync_available, 'available'):
        try:
            subprocess.run(["rsync", "--version"],
                           stdout=subprocess.DEVNULL,
                           stderr=subprocess.DEVNULL,
                           check=True)
            check_rsync_available.available = True
        except:
            check_rsync_available.available = False
    return check_rsync_available.available


def get_dir_mtime(path):
    """Get directory last modification time (recursively)"""
    if not os.path.exists(path):
        return 0

    max_mtime = os.path.getmtime(path)

    for root, dirs, files in os.walk(path):
        for f in files:
            try:
                file_mtime = os.path.getmtime(os.path.join(root, f))
                if file_mtime > max_mtime:
                    max_mtime = file_mtime
            except:
                continue

    return max_mtime


def rotate_robocopy_log():
    """Check and rotate log file in necessary"""
    if not os.path.exists(ROBOCOPY_LOG):
        return

    # Check log file size
    if os.path.getsize(ROBOCOPY_LOG) < MAX_ROBOCOPY_LOG_SIZE:
        return

    # Generate new file name for old log file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    rotated_log = f"robocopy_{timestamp}.log"

    try:
        # Rename current log file
        os.rename(ROBOCOPY_LOG, rotated_log)

        # Delete old log files
        log_files = sorted(
            [f for f in os.listdir() if f.startswith("robocopy_") and f.endswith(".log")],
            key=os.path.getctime
        )

        while len(log_files) >= MAX_LOG_FILES:
            os.remove(log_files.pop(0))

    except Exception as e:
        print(f"Log rotation error: {e}")


def copy_with_robocopy(src, dst, logger):
    """Copy using Robocopy (without file deletion)"""
    try:
        # Check logs
        rotate_robocopy_log()

        result = subprocess.run(
            [
                "robocopy", src, dst,
                "/E",  # Copy subdirectories
                "/COPY:DAT",  # Copy data, attributes and timestamps
                "/XO",  # Copy newer files only
                "/XN",  # Don't overwrite newer files
                "/MT:1",  # Single-threaded mode
                "/R:1",  # 1 retry attempt
                "/W:1",  # Wait 1 sec between retries
                "/NP",  # Don't show progress percentage
                # "/NFL",  # Don't log file names
                # "/NDL",  # Don't log directory names
                f"/LOG+:{ROBOCOPY_LOG}",  # Log to separate file
                "/TEE",  # Output to console
                "/XF", "Thumbs.db", "*.tmp",
                "/XD", "$RECYCLE.BIN", "System Volume Information"
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=3600
        )

        if result.returncode <= 7:
            status_msg = {
                0: "No files copied (source and destination synchronized)",
                1: "Files copied successfully",
                2: "Extra files detected in destination",
                3: "Copy incomplete (mismatched files)",
                4: "Some files could not be copied",
                5: "Copy incomplete (retry limit exceeded)",
                6: "Some files could not be copied (retry limit exceeded)",
                7: "Files copied, some mismatched files or retries"
            }.get(result.returncode, "Copy completed with warnings")

            logger.info(f"Robocopy status: {status_msg} (return code {result.returncode})")
            return True
        else:
            logger.error(f"Robocopy failed with error (return code {result.returncode}): {result.stderr}")
            return False

    except subprocess.TimeoutExpired:
        logger.error(f"Copy timeout exceeded: {src}")
        return False
    except Exception as e:
        logger.error(f"Robocopy error: {str(e)}")
        return False


def copy_with_rsync(src, dst, logger):
    """Copy using rsync with verification"""
    try:
        if not src.endswith('/'):
            src += '/'

        result = subprocess.run(
            ["rsync", "-avh", "--progress", "--update", "--itemize-changes", src, dst],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=3600
        )

        if result.returncode == 0:
            changes = [line for line in result.stdout.splitlines() if line.startswith('>')]
            if changes:
                logger.info(f"Changes:\n" + "\n".join(changes))
            else:
                logger.info("Files are up to date, no copying needed")
            return True
        else:
            logger.error(f"Rsync error: {result.stderr}")
            return False
    except Exception as e:
        logger.error(f"Rsync execution error: {e}")
        return False


def copy_with_python(src, dst, logger):
    """Fallback copy method using pure Python"""
    try:
        if not os.path.exists(dst):
            os.makedirs(dst, exist_ok=True)

        copied_files = 0
        skipped_files = 0
        for item in os.listdir(src):
            src_path = os.path.join(src, item)
            dst_path = os.path.join(dst, item)

            if os.path.isdir(src_path):
                sub_copied, sub_skipped = copy_with_python(src_path, dst_path, logger)
                copied_files += sub_copied
                skipped_files += sub_skipped
            else:
                if (not os.path.exists(dst_path) or
                        (os.path.getmtime(src_path) > os.path.getmtime(dst_path)) or
                        (os.path.getsize(src_path) != os.path.getsize(dst_path))):
                    shutil.copy2(src_path, dst_path)
                    logger.debug(f"Copied: {src_path}")
                    copied_files += 1
                else:
                    skipped_files += 1

                logger.info(f"Total: {copied_files} copied, {skipped_files} skipped")
        return (copied_files, skipped_files)

    except Exception as e:
        logger.error(f"Copy error: {e}")
        return (0, 0)


def copy_files(src, dst, logger):
    """Main copy function with change verification"""
    try:
        # Get current source modification time
        current_mtime = get_dir_mtime(src)
        last_copy_path = os.path.join(dst, LAST_COPY_FILE)

        if platform.system() == "Windows":
            ctypes.windll.kernel32.SetFileAttributesW(last_copy_path, 2)

        # Check if copying is needed
        need_copy = True
        if os.path.exists(last_copy_path):
            try:
                with open(last_copy_path, 'r') as f:
                    last_mtime = float(f.read())
                    if current_mtime <= last_mtime:
                        logger.info("No changes detected, copying not required")
                        need_copy = False
            except:
                pass

        if need_copy:
            logger.info(f"Changes detected in {src}, starting copy...")
            start_time = time.time()

            success = False
            if platform.system() == "Windows" and is_robocopy_available():
                logger.info("Using Robocopy for copying")
                success = copy_with_robocopy(src, dst, logger)
            else:
                if platform.system() != "Windows" and check_rsync_available():
                    logger.info("Using Rsync for copying")
                    success = copy_with_rsync(src, dst, logger)
                else:
                    logger.info("Using Python for copying")
                    copied, _ = copy_with_python(src, dst, logger)
                    success = copied > 0

            # Save last copy time only on success
            if success:
                try:
                    if platform.system() == "Windows":
                        ctypes.windll.kernel32.SetFileAttributesW(last_copy_path, 0x80)
                    with open(last_copy_path, 'w') as f:
                        f.write(str(current_mtime))
                    if platform.system() == "Windows":
                        ctypes.windll.kernel32.SetFileAttributesW(last_copy_path, 2)
                except:
                    logger.warning("Failed to save last copy time")

            elapsed = time.time() - start_time
            status = "successfully" if success else "with errors"
            logger.info(f"Copy completed {status} in {elapsed:.2f} seconds")

    except Exception as e:
        logger.error(f"Copy error: {e}")


def read_config(logger):
    """Read configuration with validation"""
    config = {
        'disk_name': '',
        'scan_interval': 300,
        'copy_pairs': []
    }

    required_fields = ['disk_name', 'copy_pairs']

    try:
        with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
            logger.info("=== Loading configuration ===")

            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue

                if '=' in line:
                    key, value = map(str.strip, line.split('=', 1))
                    key = key.upper()

                    if key == 'DRIVE_LABEL':
                        config['disk_name'] = value
                    elif key == 'SCAN_INTERVAL':
                        try:
                            config['scan_interval'] = max(10, int(value))  # Minimum 10 seconds
                        except ValueError:
                            logger.warning(f"Invalid interval, using default {config['scan_interval']} sec")

                elif '->' in line:
                    src, dst = map(str.strip, line.split('->', 1))
                    if os.path.exists(src):
                        config['copy_pairs'].append({'source': src, 'destination': dst})
                    else:
                        logger.warning(f"Source doesn't exist: {src}")

        # Validate required fields
        for field in required_fields:
            if not config[field]:
                raise ValueError(f"Required field missing: {field}")

        logger.info("=== Configuration loaded ===")
        return config

    except FileNotFoundError:
        logger.error(f"Configuration file {CONFIG_FILE} not found!")
        raise
    except Exception as e:
        logger.error(f"Configuration error: {str(e)}")
        raise


def main():
    """Main function with task scheduler"""
    logger = setup_logging()
    logger.info("\n=== Starting backup service ===")

    try:
        config = read_config(logger)
        logger.info(f"Configuration:\n"
                    f"Disk label: {config['disk_name']}\n"
                    f"Scan interval: {config['scan_interval']} sec\n"
                    f"Copy paths: {config['copy_pairs']}")

        # Check copy tools availability
        if platform.system() == "Windows":
            logger.info(f"Robocopy available: {'Yes' if is_robocopy_available() else 'No'}")
        else:
            logger.info(f"Rsync available: {'Yes' if check_rsync_available() else 'No'}")

        def job():
            """Scheduled job"""
            try:
                disk_path = is_disk_connected(config['disk_name'])
                if disk_path:
                    logger.info(f"Disk {config['disk_name']} connected: {disk_path}")
                    for pair in config['copy_pairs']:
                        copy_files(pair['source'], os.path.join(disk_path, pair['destination']), logger)
                else:
                    logger.info(f"Disk {config['disk_name']} not connected")
            except Exception as e:
                logger.error(f"Copy job error: {str(e)}")

        # Schedule setup
        schedule.every(config['scan_interval']).seconds.do(job)

        # First run immediately
        job()

        # Main schedule loop
        while True:
            schedule.run_pending()
            time.sleep(1)

    except KeyboardInterrupt:
        logger.info("Interrupt signal received")
    except Exception as e:
        logger.error(f"Critical error: {str(e)}")
    finally:
        schedule.clear()
        logger.info("=== Service stopped ===\n")


if __name__ == "__main__":
    main()
