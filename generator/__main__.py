from generator.generate import generate_all
from generator.upload import upload_all

if __name__ == "__main__":
    dataframes = generate_all()
    upload_all(dataframes)
