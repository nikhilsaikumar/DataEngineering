import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node albums
albums_node1712441439727 = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": '"', "withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://spotifybucket4/staging/albums.csv"],
        "recurse": True,
    },
    transformation_ctx="albums_node1712441439727",
)

# Script generated for node artists
artists_node1712441441364 = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": '"', "withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://spotifybucket4/staging/artists.csv"],
        "recurse": True,
    },
    transformation_ctx="artists_node1712441441364",
)

# Script generated for node track
track_node1712441442512 = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": '"', "withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://spotifybucket4/staging/track.csv"],
        "recurse": True,
    },
    transformation_ctx="track_node1712441442512",
)

# Script generated for node Join Artist and Album
JoinArtistandAlbum_node1712441533704 = Join.apply(
    frame1=artists_node1712441441364,
    frame2=albums_node1712441439727,
    keys1=["id"],
    keys2=["artist_id"],
    transformation_ctx="JoinArtistandAlbum_node1712441533704",
)

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1712442721317 = ApplyMapping.apply(
    frame=track_node1712441442512,
    mappings=[
        ("track_id", "string", "right_track_id", "string"),
        ("track_popularity", "string", "right_track_popularity", "string"),
    ],
    transformation_ctx="RenamedkeysforJoin_node1712442721317",
)

# Script generated for node Join with tracks
Joinwithtracks_node1712442636297 = Join.apply(
    frame1=JoinArtistandAlbum_node1712441533704,
    frame2=RenamedkeysforJoin_node1712442721317,
    keys1=["track_id"],
    keys2=["right_track_id"],
    transformation_ctx="Joinwithtracks_node1712442636297",
)

# Script generated for node Drop Fields
DropFields_node1712442755876 = DropFields.apply(
    frame=Joinwithtracks_node1712442636297,
    paths=["right_track_id", "artist_id"],
    transformation_ctx="DropFields_node1712442755876",
)

# Script generated for node Destination
Destination_node1712442855617 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1712442755876,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://spotifybucket4/datawarehouse/",
        "partitionKeys": [],
    },
    format_options={"compression": "snappy"},
    transformation_ctx="Destination_node1712442855617",
)

job.commit()
