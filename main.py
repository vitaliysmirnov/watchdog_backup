import os
import shutil
import time
from datetime import datetime, timedelta
from tqdm import tqdm
from humanize import naturalsize

# Константы
CONFIG_FILE = "config.txt"
CHECK_INTERVAL = 30
LOG_RETENTION_DAYS = 10
LOG_DIR = os.path.expanduser("~\\watchdog_backup")
LOG_FILE = os.path.join(LOG_DIR, "watchdog_backup.log")


def load_config():
    """Загружает конфигурацию из файла"""
    try:
        with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
            lines = [line.strip() for line in f if line.strip()]

        if not lines:
            raise ValueError("Файл конфигурации пуст")

        # Первая строка - метка диска
        if '=' not in lines[0]:
            raise ValueError("Первая строка должна содержать DRIVE_LABEL=метка_диска")

        drive_label = lines[0].split('=', 1)[1].strip()

        # Остальные строки - пары source->destination
        source_dirs = []
        target_dirs = []

        for line in lines[1:]:
            if '->' not in line:
                continue

            src, dst = map(str.strip, line.split('->', 1))
            source_dirs.append(src)
            target_dirs.append(dst)

        if not source_dirs:
            raise ValueError("Не указаны директории для копирования")

        return drive_label, source_dirs, target_dirs

    except Exception as e:
        raise ValueError(f"Ошибка чтения конфига: {str(e)}")


class LogRotator:
    def __init__(self, log_file, retention_days):
        self.log_file = log_file
        self.retention_days = retention_days
        self.last_rotation_date = datetime.now().date()

    def needs_rotation(self):
        current_date = datetime.now().date()
        if current_date != self.last_rotation_date and datetime.now().hour == 0:
            self.last_rotation_date = current_date
            return True
        return False

    def rotate_logs(self):
        if not os.path.exists(self.log_file):
            return

        os.makedirs(os.path.dirname(self.log_file), exist_ok=True)
        current_date = datetime.now().strftime("%Y-%m-%d")
        archived_log = f"{self.log_file}.{current_date}"

        if not os.path.exists(archived_log):
            try:
                os.rename(self.log_file, archived_log)
            except Exception as e:
                print(f"Ошибка при ротации логов: {e}")
                return

        self.cleanup_old_logs()

    def cleanup_old_logs(self):
        cutoff_date = datetime.now() - timedelta(days=self.retention_days)
        log_dir = os.path.dirname(self.log_file)

        for filename in os.listdir(log_dir):
            if filename.startswith(os.path.basename(self.log_file)):
                try:
                    file_path = os.path.join(log_dir, filename)
                    if filename.count('.') >= 2:
                        file_date_str = filename.split('.')[-2]
                        file_date = datetime.strptime(file_date_str, "%Y-%m-%d").date()
                        if file_date < cutoff_date.date():
                            os.remove(file_path)
                except Exception as e:
                    print(f"Ошибка при удалении старого лога {filename}: {e}")


class Logger:
    def __init__(self, log_file=None, retention_days=10):
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        self.log_file = log_file
        self.log_rotator = LogRotator(log_file, retention_days)

    def log(self, message, console=True):
        if self.log_rotator.needs_rotation():
            self.log_rotator.rotate_logs()

        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_message = f"[{timestamp}] {message}"

        if console:
            print(log_message)

        if self.log_file:
            try:
                with open(self.log_file, "a", encoding="utf-8") as f:
                    f.write(log_message + "\n")
            except Exception as e:
                print(f"Ошибка записи в лог: {e}")


class BackupManager:
    def __init__(self, logger, drive_label, source_dirs, target_dirs):
        self.logger = logger
        self.drive_label = drive_label
        self.source_dirs = source_dirs
        self.target_dirs = target_dirs
        self.last_sync_time = 0
        self.drive_connected = False

    def find_drive_path(self):
        try:
            if os.name == 'nt':
                drive_info = os.popen("wmic logicaldisk get caption,volumename").read()
                for line in drive_info.split('\n'):
                    if line.strip():
                        parts = line.split()
                        if len(parts) >= 2 and parts[1] == self.drive_label:
                            return parts[0] + "\\"
            else:
                drive_info = os.popen("lsblk -o LABEL,MOUNTPOINT -n").read()
                for line in drive_info.split('\n'):
                    if line.strip():
                        parts = line.split(maxsplit=1)
                        if len(parts) >= 2 and parts[0] == self.drive_label:
                            return parts[1]
            return None
        except Exception as e:
            self.logger.log(f"Ошибка поиска диска: {e}")
            return None

    def check_drive_connection(self):
        drive_path = self.find_drive_path()
        if drive_path:
            for target_dir in self.target_dirs:
                if not os.path.exists(
                        os.path.join(drive_path, os.path.relpath(target_dir, target_dir.split(os.sep)[0] + os.sep))):
                    return False
            return True
        return False

    def sync_directories(self):
        self.logger.log("\n" + "=" * 50)
        self.logger.log(f"Начало синхронизации")

        for src, dst in zip(self.source_dirs, self.target_dirs):
            if not os.path.exists(src):
                self.logger.log(f"⚠️ Исходная директория {src} не найдена, пропускаем")
                continue

            self.logger.log(f"\n🔍 Синхронизация {src} → {dst}")
            os.makedirs(dst, exist_ok=True)

            files_to_copy = []
            total_size = 0
            copied_files = 0
            skipped_files = 0

            for root, _, files in os.walk(src):
                for file in files:
                    src_path = os.path.join(root, file)
                    rel_path = os.path.relpath(src_path, src)
                    dst_path = os.path.join(dst, rel_path)

                    need_copy = False
                    if not os.path.exists(dst_path):
                        need_copy = True
                    else:
                        try:
                            src_stat = os.stat(src_path)
                            dst_stat = os.stat(dst_path)
                            if (src_stat.st_size != dst_stat.st_size or
                                    src_stat.st_mtime > dst_stat.st_mtime):
                                need_copy = True
                        except OSError:
                            need_copy = True

                    if need_copy:
                        try:
                            file_size = os.path.getsize(src_path)
                            files_to_copy.append((src_path, dst_path, file_size))
                            total_size += file_size
                        except OSError as e:
                            self.logger.log(f"⚠️ Ошибка доступа к файлу {src_path}: {e}")
                    else:
                        skipped_files += 1

            self.logger.log(f"📋 Найдено {len(files_to_copy)} файлов для копирования")
            self.logger.log(f"📦 Общий объем: {naturalsize(total_size)}")
            self.logger.log(f"⏭ Пропущено {skipped_files} файлов (уже синхронизированы)")

            if files_to_copy:
                with tqdm(total=total_size, unit='B', unit_scale=True, unit_divisor=1024,
                          desc="🔄 Копирование", bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{rate_fmt}]') as pbar:
                    for src_path, dst_path, file_size in files_to_copy:
                        try:
                            os.makedirs(os.path.dirname(dst_path), exist_ok=True)
                            shutil.copy2(src_path, dst_path)
                            copied_files += 1
                        except Exception as e:
                            self.logger.log(f"⚠️ Ошибка при копировании {src_path}: {e}")
                        finally:
                            pbar.update(file_size)

                self.logger.log(f"✅ Успешно скопировано {copied_files} файлов ({naturalsize(total_size)})")
            else:
                self.logger.log("👍 Нет файлов для копирования (все актуально)")

        self.last_sync_time = time.time()
        self.logger.log("\n" + "=" * 50)


def main():
    try:
        # Загрузка конфигурации
        DRIVE_LABEL, SOURCE_DIRS, TARGET_DIRS = load_config()

        # Инициализация логгера
        os.makedirs(LOG_DIR, exist_ok=True)
        logger = Logger(LOG_FILE, LOG_RETENTION_DAYS)

        logger.log("""
░▒▓█▓▒░░▒▓█▓▒░░▒▓█▓▒░░▒▓██████▓▒░▒▓████████▓▒░▒▓██████▓▒░░▒▓█▓▒░░▒▓█▓▒░▒▓███████▓▒░ ░▒▓██████▓▒░ ░▒▓██████▓▒░  
░▒▓█▓▒░░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░░▒▓█▓▒░ ░▒▓█▓▒░  ░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░░▒▓█▓▒░ 
░▒▓█▓▒░░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░░▒▓█▓▒░ ░▒▓█▓▒░  ░▒▓█▓▒░      ░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░        
░▒▓█▓▒░░▒▓█▓▒░░▒▓█▓▒░▒▓████████▓▒░ ░▒▓█▓▒░  ░▒▓█▓▒░      ░▒▓████████▓▒░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒▒▓███▓▒░ 
░▒▓█▓▒░░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░░▒▓█▓▒░ ░▒▓█▓▒░  ░▒▓█▓▒░      ░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░░▒▓█▓▒░ 
░▒▓█▓▒░░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░░▒▓█▓▒░ ░▒▓█▓▒░  ░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░░▒▓█▓▒░ 
 ░▒▓█████████████▓▒░░▒▓█▓▒░░▒▓█▓▒░ ░▒▓█▓▒░   ░▒▓██████▓▒░░▒▓█▓▒░░▒▓█▓▒░▒▓███████▓▒░ ░▒▓██████▓▒░ ░▒▓██████▓▒░  
 
░▒▓███████▓▒░ ░▒▓██████▓▒░ ░▒▓██████▓▒░░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░░▒▓█▓▒░▒▓███████▓▒░  
░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░░▒▓█▓▒░ 
░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░      ░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░░▒▓█▓▒░ 
░▒▓███████▓▒░░▒▓████████▓▒░▒▓█▓▒░      ░▒▓███████▓▒░░▒▓█▓▒░░▒▓█▓▒░▒▓███████▓▒░  
░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░      ░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░        
░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░        
░▒▓███████▓▒░░▒▓█▓▒░░▒▓█▓▒░░▒▓██████▓▒░░▒▓█▓▒░░▒▓█▓▒░░▒▓██████▓▒░░▒▓█▓▒░                                                                                                                                                                                                    
        """)
        logger.log(f"🔧 Конфигурация:")
        logger.log(f"Метка диска: {DRIVE_LABEL}")
        logger.log(f"Исходные директории: {SOURCE_DIRS}")
        logger.log(f"Целевые директории: {TARGET_DIRS}")
        logger.log(f"Интервал проверки: {CHECK_INTERVAL} сек")
        logger.log(f"Лог-файл: {LOG_FILE}")
        logger.log(f"Хранение логов: {LOG_RETENTION_DAYS} дней\n")
        logger.log("🚀 Скрипт запущен. Ожидание подключения диска...")

        # Инициализация менеджера бэкапов
        backup_manager = BackupManager(logger, DRIVE_LABEL, SOURCE_DIRS, TARGET_DIRS)

        # Основной цикл
        while True:
            current_drive_state = backup_manager.check_drive_connection()

            if current_drive_state != backup_manager.drive_connected:
                if current_drive_state:
                    logger.log("\n🎯 Внешний диск подключен!")
                    backup_manager.sync_directories()
                else:
                    logger.log("\n🔌 Диск отключен")
                backup_manager.drive_connected = current_drive_state

            elif (backup_manager.drive_connected and
                  time.time() - backup_manager.last_sync_time > CHECK_INTERVAL):
                logger.log("\n🔄 Запуск периодической синхронизации...")
                backup_manager.sync_directories()

            time.sleep(5)

    except Exception as e:
        print(f"Ошибка: {str(e)}")
        input("Нажмите Enter для выхода...")


if __name__ == "__main__":
    main()
