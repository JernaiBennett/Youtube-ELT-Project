import logging

logger = logging.getLogger(__name__)
table = "yt_api"

def insert_rows(cur, conn, schema, row):
    try:

        # staging rows come from the raw YouTube API payload, so the dict keys
        # are the API's camelCase field names rather than the warehouse column names
        if schema == 'staging':

            video_id = 'video_id'

            cur.execute(
                f"""INSERT INTO {schema}.{table}(
                    "Video_ID",
                    "Video_Title",
                    "Upload_Date",
                    "Duration",
                    "Video_Views",
                    "Likes_Count",
                    "Comments_Count"
                )
                VALUES (%(video_id)s,
                %(title)s,
                %(publishedAt)s,
                %(duration)s,
                %(viewCount)s,
                %(likeCount)s,
                %(commentCount)s);
                """, row
            )
    
        else:

            # core/warehouse rows already use the warehouse's own column names
            video_id = 'Video_ID'

            cur.execute(
                f"""INSERT INTO {schema}.{table}(
                    "Video_ID",
                    "Video_Title",
                    "Upload_Date",
                    "Duration",
                    "Video_Views",
                    "Likes_Count",
                    "Comments_Count"
                )
                VALUES (%(Video_ID)s,
                %(Video_Title)s,
                %(Upload_Date)s,
                %(Duration)s,
                %(Video_Views)s,
                %(Likes_Count)s,
                %(Comments_Count)s);
                """, row
            )
        
        conn.commit()

        logger.info(f"Inserted row with Video_ID: {row[video_id]}")

    except Exception as e:
        logger.error(f"Error inserting row with Video_ID: {row[video_id]} - {e}")
        raise e

def update_rows(cur, conn, schema, row):

    try:

        # map schema-specific dict keys onto the names used in the query below,
        # since staging (API) and core (warehouse) rows use different casing/naming
        #staging
        if schema == 'staging':
            video_id = 'video_id'
            upload_date = 'publishedAt'
            video_title = 'title'
            video_views = 'viewCount'
            likes_count = 'likeCount'
            comments_count = 'commentCount'
        
        #core
        else:
            
            video_id = 'Video_Id'
            upload_date = 'Upload_Date'
            video_title = 'Video_title'
            video_views =  'Video_Views'
            likes_count = 'Likes_Count'
            comments_count = 'Comments_Count'

        # bind-parameter placeholders (%(name)s) are filled in by psycopg2 from `row`;
        # the {name} parts are f-string interpolation that substitutes in the
        # *key name* to look up in `row`, not the value itself
        cur.execute(
            f"""
                UPDATE {schema}.{table}
                SET "Video_Title" = %({video_title})s,
                    "Video_Views" = %({video_views})s,
                    "Likes_Count" = %({likes_count})s,
                    "Comments_Count" = %({comments_count})s,
                WHERE "Video_ID" = %({video_id})s AND "Upload_Date" = %({upload_date})s;  
            """, row,
        )

        conn.commit()

        logger.info(f"Updated row with Video_ID: {row[video_id]}")

    except Exception as e:
        logger.error(f"Error updating row with Video_ID: {row[video_id]} - {e}")
        raise e

def delete_rows(cur, conn, schema, ids_to_delete):
    try:

        # build a literal SQL IN-list, e.g. ('abc123','def456'), since psycopg2's
        # %s placeholders can't bind a variable-length list of values directly
        ids_to_delete = f"""({','.join(f"'{id}'" for id in ids_to_delete)})"""

        cur.execute(
            f"""
            DELETE FROM {schema}.{table}
            WHERE "Video_ID" IN {ids_to_delete};
            """
        )

        conn.commit()

        logger.info(f"Deleted row with Video_ID: {ids_to_delete}")

    except Exception as e:
        logger.error(f"Error deleting row with Video_ID: {ids_to_delete} - {e}")
        raise e


