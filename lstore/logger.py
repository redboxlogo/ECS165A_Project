import logging

# It's a good practice to separate your logging configuration into a separate module or file, especially if your project grows larger. 
# This helps in keeping your code modular and organized. 
# You can  import the logger module into your main script or any other modules where you need logging functionality.


# Set up logging configuration
logging.basicConfig(filename='database.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Create logger object
logger = logging.getLogger(__name__)

# Define additional functions if needed
def log_info(message):
    logger.info(message)

def log_error(message):
    logger.error(message)

def log_warning(message):
    logger.warning(message)