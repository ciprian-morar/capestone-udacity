config = configparser.ConfigParser()
config.read('../aws/cred.cfg')
os.environ["AWS_ACCESS_KEY_ID"] = config.get("AWS", "AWS_ACCESS_KEY_ID")
os.environ["AWS_SECRET_ACCESS_KEY"] = config.get("AWS", "AWS_SECRET_ACCESS_KEY")

s3_client = boto3.client('s3')
s3_bucket = 'capestone-project-udacity-ciprian'
s3_key = "data"

def uploadEmrScripts():
    relative_path = "scripts/spark/"
    files = [relative_path + f for f in os.listdir(relative_path)]
    for f in files:
        uploadToS3(f, "/emr/" + f.split("/")[-1], "scripts")

def uploadToS3(file,s3_file_name, s3_dir=None):
    # global s3_key
    try:
        if s3_dir is not None:
            s3_key = s3_dir
        s3_client.upload_file(file, s3_bucket, s3_key + s3_file_name)
    except Exception as e:
        print(e)

def uploadImmigrationDataToS3():
    relative_path = "data/source/i94_immigration_data/18-83510-I94-Data-2016/"
    files = [relative_path + f for f in os.listdir(relative_path)]
    for f in files:
        uploadToS3(f,"/source/i94_immigration_data/18-83510-I94-Data-2016/" + f.split("/")[-1], "data")

def uploadGlobalTemperatures():
    relative_path = "data/source/temperature/GlobalLandTemperaturesByCity.csv"
    uploadToS3(relative_path, "/source/temperature/GlobalLandTemperaturesByCity.csv", "data")

def uploadDemographics():
    uploadToS3("data/source/us-cities-demographics.csv","/source/demographics/us-cities-demographics.csv", "data")


def uploadAirportCodes():
    uploadToS3("data/source/airport-codes_csv.csv", "/source/airport/airport-codes_csv.csv", "data")

def uploadLabels():
    uploadToS3("data/source/additional_tables/i94addrl.txt", "/source/labels/i94addrl.txt", "data")
    uploadToS3("data/source/additional_tables/i94cntyl.txt", "/source/labels/i94cntyl.txt", "data")
    uploadToS3("data/source/additional_tables/i94prtl.txt", "/source/labels/i94prtl.txt", "data")
    uploadToS3("data/source/additional_tables/i94model.txt", "/source/labels/i94model.txt", "data")
    uploadToS3("data/source/additional_tables/i94visa.txt", "/source/labels/i94visa.txt", "data")


# def uploadSepparateImmigrationDataToS3():
#     relative_path = "data/source/i94_immigration_data/18-83510-I94-Data-2016/i94_jun16_sub.sas7bdat"
#     uploadToS3(relative_path,"/source/i94_immigration_data/18-83510-I94-Data-2016/i94_jun16_sub.sas7bdat")


if __name__ == "__main__":
    uploadEmrScripts()
    # uploadSepparateImmigrationDataToS3()
    uploadImmigrationDataToS3()
    uploadDemographics()
    uploadGlobalTemperatures()
    uploadAirportCodes()
    uploadLabels()