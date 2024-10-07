import subprocess
import datetime

def backup_to_gdrive(local_directory, gdrive_directory):
    """
    Backs up a local directory to Google Drive using rclone.
    
    :param local_directory: Path to the local directory to be backed up.
    :param gdrive_directory: Path to the Google Drive directory.
    """
    # timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    backup_command = [
        "rclone", "sync", 
        local_directory, 
        f"mybackupdrive:{gdrive_directory}"
    ]

    try:
        subprocess.run(backup_command, check=True)
        print(f"Backup successful to {gdrive_directory}")
    except subprocess.CalledProcessError as e:
        print(f"Error during backup: {e}")

if __name__ == "__main__":
    local_directory = "backups/OrderBookData"
    gdrive_directory = "DATA_BACKUP/Stock_OrderBookData"  # This should match the directory name in your Google Drive
    
    backup_to_gdrive(local_directory, gdrive_directory)
