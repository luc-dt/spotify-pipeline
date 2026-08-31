from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    BooleanType,
    ArrayType,
)

# build the 3 schema

RAW_ARTISTS_SCHEMA = StructType([
    StructField("artist_id", StringType(), nullable=False),
    StructField("artist_name", StringType(), nullable=False),
    StructField("spotify_uri", StringType(), nullable=True),
    StructField("image_url", StringType(), nullable=True),
    StructField("genres", ArrayType(StringType()), nullable=True),
    StructField("extracted_at", StringType(), nullable=True),
    StructField("snapshot_date", StringType(), nullable=False),
])

RAW_ALBUMS_SCHEMA = StructType([
    StructField("album_id", StringType(), nullable=False),
    StructField("album_name", StringType(), nullable=False),
    StructField("album_type", StringType(), nullable=True),
    StructField("release_date", StringType(), nullable=True),
    StructField("release_date_precision", StringType(), nullable=True),
    StructField("total_tracks", IntegerType(), nullable=True),
    StructField("artist_id", StringType(), nullable=False),
    StructField("artist_name", StringType(), nullable=True),
    StructField("spotify_uri", StringType(), nullable=True),
    StructField("image_url", StringType(), nullable=True),
    StructField("extracted_at", StringType(), nullable=True),
    StructField("snapshot_date", StringType(), nullable=False),
])

RAW_TRACKS_SCHEMA = StructType([
    StructField("track_id", StringType(), nullable=False),
    StructField("track_name", StringType(), nullable=False),
    StructField("duration_ms", LongType(), nullable=True),
    StructField("explicit", BooleanType(), nullable=True),
    StructField("track_number", IntegerType(), nullable=True),
    StructField("disc_number", IntegerType(), nullable=True),
    StructField("album_id", StringType(), nullable=False),
    StructField("artist_id", StringType(), nullable=False),
    StructField("spotify_uri", StringType(), nullable=True),
    StructField("extracted_at", StringType(), nullable=True),
    StructField("snapshot_date", StringType(), nullable=False),
])

# mapping dictionary for Dynamic Retrieval

ENTITY_SCHEMAS = {
    "artists": RAW_ARTISTS_SCHEMA,
    "albums": RAW_ALBUMS_SCHEMA,
    "tracks": RAW_TRACKS_SCHEMA,
}

if __name__ == "__main__":
    print("=" * 60)
    print("📋 Testing StructType Schema Definitions...")
    print("=" * 60)
    for entity, schema in ENTITY_SCHEMAS.items():
        print(f"\n🔹 Entity: {entity.upper()} ({len(schema.fields)} fields)")
        for field in schema.fields:
            null_str = "nullable" if field.nullable else "NOT NULL"
            print(f"   • {field.name:<25} : {field.dataType.simpleString():<20} [{null_str}]")
