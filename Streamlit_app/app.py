import os
import time
import json
from collections import Counter
from datetime import datetime, timedelta
from datetime import time as dt_time

import pandas as pd
from io import BytesIO
import plotly.express as px
import psycopg2
import pycountry
import streamlit as st
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions, ClusterTimeoutOptions

from cache import TTLCache, RedisCache, LRUCache

endpoint = "cb.u5tbreifenk4gngi.cloud.couchbase.com"
username = "DBMS"
password = "Dbms@123"
bucket_name = "dbmsProject"
scope_name = "twitter"
collection_name = "tweets"

PGHOST = "twitter.postgres.database.azure.com"
PGUSER = "neeraj"
PGPORT = 5432
PGDATABASE = "postgres"
PGPASSWORD = "Dbms@123"

authenticator = PasswordAuthenticator(username, password)
timeout_opts = ClusterTimeoutOptions(
    connect_timeout=timedelta(seconds=60), kv_timeout=timedelta(seconds=60)
)
options = ClusterOptions(authenticator=authenticator, timeout_options=timeout_opts)
cluster = Cluster(f"couchbases://{endpoint}", options)
cluster.wait_until_ready(timedelta(seconds=5))
bucket = cluster.bucket(bucket_name)
inventory_scope = bucket.scope(scope_name)
cb_coll = inventory_scope.collection(collection_name)


st.markdown(
    "<h1 style='text-align: center;'>TwitSeeker: Search Engine for Twitter</h1>",
    unsafe_allow_html=True,
)

# Get the current working directory
cwd = os.getcwd()

# Define the relative path
relative_path = "streamlit_app/Twitter-Gif.gif"

# Combine the current working directory with the relative path
gif_path = os.path.join(cwd, relative_path)

col1, col2, col3 = st.columns([1, 1, 1])
with col2:
    st.image(gif_path)


search_type = st.selectbox("Search by:", ("Username", "Hashtag", "Tweets"), index=0)

query = st.text_input("Enter your search query here...")


def search_by_hashtag(
    hashtag,
    start_datetime,
    end_datetime,
    sort_by,
    bucket_name,
    scope_name,
    collection_name,
):
    start_time = time.time()
    start_datetime_str = start_datetime.strftime("%Y-%m-%d %H:%M:%S+00:00")
    end_datetime_str = end_datetime.strftime("%Y-%m-%d %H:%M:%S+00:00")
    order_by = sort_options[sort_by]

    sql_query = f"""
    SELECT tweets.created_at, tweets.favorite_count, tweets.hashtags, tweets.id,
    tweets.is_retweet, tweets.original_tweet_id, tweets.reply_count,
    tweets.retweet_count, tweets.retweeted_status, tweets.text, tweets.urls, tweets.user_id
    FROM `{bucket_name}`.`{scope_name}`.`{collection_name}`
    WHERE tweets.hashtags LIKE '%{hashtag}%'
    AND tweets.created_at BETWEEN '{start_datetime_str}' AND '{end_datetime_str}'
    ORDER BY {order_by};
    """

    try:
        row_iter = inventory_scope.query(sql_query)
        results = [row for row in row_iter]
        df = pd.DataFrame(results)
        elapsed_time = time.time() - start_time
        total_count = len(df)
        return df, total_count, elapsed_time
    except Exception as e:
        print("Error during database query: " + str(e))
        return pd.DataFrame(), 0, 0


def search_by_text(
    search_text,
    start_datetime,
    end_datetime,
    sort_by,
    bucket_name,
    scope_name,
    collection_name,
):
    start_time = time.time()

    like_pattern = f"%{search_text}%"
    start_datetime_str = start_datetime.strftime("%Y-%m-%d %H:%M:%S+00:00")
    end_datetime_str = end_datetime.strftime("%Y-%m-%d %H:%M:%S+00:00")
    order_by = sort_options[sort_by]

    sql_query = f"""
    SELECT tweets.created_at, tweets.favorite_count, tweets.hashtags, tweets.id,
        tweets.is_retweet, tweets.original_tweet_id, tweets.reply_count,
        tweets.retweet_count, tweets.retweeted_status, tweets.text, tweets.urls, tweets.user_id
    FROM `{bucket_name}`.`{scope_name}`.`{collection_name}`
    WHERE tweets.text LIKE '{like_pattern}'
    AND tweets.created_at BETWEEN '{start_datetime_str}' AND '{end_datetime_str}'
    ORDER BY {order_by};
    """

    try:
        row_iter = inventory_scope.query(sql_query)
        results = []
        if row_iter:
            results = [row for row in row_iter]
        df = pd.DataFrame(results)

        df = df.drop_duplicates(subset="id")
        df["View Retweets"] = df["retweet_count"].apply(
            lambda x: "View" if x > 0 else ""
        )

        elapsed_time = time.time() - start_time
        total_count = len(df)

        return df, total_count, elapsed_time
    except Exception as e:
        print("Error during database query: " + str(e))
        return pd.DataFrame(), 0, 0


def search_by_username(username):
    conn = psycopg2.connect(
        dbname=PGDATABASE, user=PGUSER, password=PGPASSWORD, host=PGHOST, port=PGPORT
    )
    cur = conn.cursor()

    cur.execute(
        "SELECT id, name, screen_name, location, url, followers_count, friends_count, statuses_count, verified, created_at FROM users_final WHERE name = %s",
        (username,),
    )
    user_details = cur.fetchone()

    if not user_details:
        cur.close()
        conn.close()
        return "User not found.", pd.DataFrame(), 0

    user_data = {
        "ID": user_details[0],
        "Name": user_details[1],
        "Screen Name": user_details[2],
        "Location": user_details[3],
        "URL": user_details[4],
        "Followers Count": user_details[5],
        "Friends Count": user_details[6],
        "Statuses Count": user_details[7],
        "Verified": user_details[8],
        "Created At": user_details[9],
    }

    start_time = time.time()
    tweet_query = f"""
    SELECT tweets.id, tweets.text, tweets.user_id
    FROM `{bucket_name}`.`{scope_name}`.`{collection_name}`
    WHERE tweets.user_id = {user_data["ID"]};
    """
    try:
        tweet_results = inventory_scope.query(tweet_query)
        results = [row for row in tweet_results]
        df = pd.DataFrame(results)
        df = df.drop_duplicates(subset="id")
        elapsed_time = time.time() - start_time
        cur.close()
        conn.close()
        return user_data, df, elapsed_time
    except Exception as e:
        st.error("Error during database tweet query: " + str(e))
        cur.close()
        conn.close()
        return {}, pd.DataFrame(), 0


def get_tweets_by_user(user_id, bucket_name, scope_name, collection_name):
    sql_query = f"""
    SELECT tweets.created_at, tweets.text, tweets.user_id
    FROM `{bucket_name}`.`{scope_name}`.`{collection_name}`
    WHERE tweets.user_id = {user_id}
    ORDER BY tweets.created_at DESC;
    """
    try:
        row_iter = inventory_scope.query(sql_query)
        results = [row for row in row_iter]
        users_df = pd.DataFrame(results)
        users_df = users_df.drop_duplicates(subset=["created_at"])
        return users_df
    except Exception as e:
        print(f"Error fetching tweets for user {user_id}: {str(e)}")
        return pd.DataFrame()
    finally:
        pass


def get_retweets_info(original_tweet_id, inventory_scope):
    try:
        query = f"""
        SELECT t.created_at AS retweet_time, t.user_id, t.text AS retweet_text
        FROM `{bucket_name}`.`{scope_name}`.`{collection_name}` AS t
        WHERE t.original_tweet_id = {original_tweet_id} AND t.is_retweet = 'True';
        """
        row_iter = inventory_scope.query(query)
        results = [row for row in row_iter] if row_iter else []
        retweets_df = pd.DataFrame(results)

        return retweets_df
    except Exception as e:
        print(
            f"Failed to retrieve retweets for the original tweet ID {original_tweet_id}: {str(e)}"
        )
        return pd.DataFrame()


@st.experimental_fragment
def check_cache(query):
    try:
        start = time.time()
        cached_result = st.session_state.inmemory_cache[query]
        elapsed_time = time.time() - start

    except KeyError:
        st.write("Cache miss.")
        cached_result = None
        elapsed_time = 0

    return cached_result, elapsed_time


default_start_date = datetime(2020, 4, 1)
default_end_date = datetime(2020, 4, 30)

col1, col2, col3, col4 = st.columns(4)
with col1:
    start_date = st.date_input(
        "Start date", value=default_start_date, key="start_date_input"
    )
with col2:
    start_time = st.time_input(
        "Start time", value=dt_time(0, 0), key="start_time_input"
    )
with col3:
    end_date = st.date_input("End date", value=default_end_date, key="end_date_input")
with col4:
    end_time = st.time_input("End time", value=dt_time(23, 59), key="end_time_input")

start_datetime = datetime.combine(start_date, start_time)
end_datetime = datetime.combine(end_date, end_time)

num_tweets_to_display = st.slider("Number of tweets to display:", 1, 3000, 5)

sort_options = {
    "Most Recent": "created_at DESC",
    "Least Recent": "created_at ASC",
    "Most Favorited": "favorite_count DESC",
    "Least Favorited": "favorite_count ASC",
    "Most Replies": "reply_count DESC",
    "Least Replies": "reply_count ASC",
}

sort_options_cached ={
    "Most Recent": ['created_at', False],
    "Least Recent": ['created_at', True],
    "Most Favorited": ['favorite_count', False],
    "Least Favorited": ['favorite_count', True],
    "Most Replies": ['reply_count', False],
    "Least Replies": ['reply_count', True],
}
selected_sort = st.selectbox("Sort by:", list(sort_options.keys()))

if st.button("Search"):
    if "results_df" not in st.session_state:
        st.session_state.results_df = pd.DataFrame()

    if "inmemory_cache" not in st.session_state:
        st.session_state.inmemory_cache = LRUCache(50)

    cached_result, cache_time = check_cache(query)

    if search_type == "Hashtag":
        if cached_result is None:
            results_df, total_count, elapsed_time = search_by_hashtag(
                query,
                start_datetime,
                end_datetime,
                selected_sort,
                bucket_name,
                scope_name,
                collection_name,
            )
            st.session_state.inmemory_cache[query] = results_df
        else:
            start = time.time()
            results_df = cached_result
            results_df = results_df.sort_values(by=sort_options_cached[selected_sort][0], ascending=sort_options_cached[selected_sort][1])
            total_count = len(results_df)
            elapsed_time = cache_time

        if not results_df.empty:
            st.write(f"Total tweets found: {total_count}")
            st.write(f"Time taken to retrieve results: {elapsed_time:.2f} seconds")
            st.write("Sample Tweets:")
            st.dataframe(results_df.head(num_tweets_to_display))
        else:
            st.write("No results found.")

    elif search_type == "Tweets":
        if cached_result is None:
            st.session_state.results_df, total_count, elapsed_time = search_by_text(
                query,
                start_datetime,
                end_datetime,
                selected_sort,
                bucket_name,
                scope_name,
                collection_name,
            )
            st.session_state.inmemory_cache[query] = st.session_state.results_df
        else:
            st.session_state.results_df = cached_result
            st.session_state.results_df = st.session_state.results_df.sort_values(by=sort_options_cached[selected_sort][0], ascending=sort_options_cached[selected_sort][1])

            elapsed_time = cache_time
            total_count = len(st.session_state.results_df)

        if not st.session_state.results_df.empty:
            st.write(f"Total tweets found: {total_count}")
            st.write(f"Time taken to retrieve results: {elapsed_time:.2f} seconds")
        else:
            st.write("No results found.")
    elif search_type == "Username":
        if cached_result is None:
            user_data, tweets_df, query_time = search_by_username(query)
            st.session_state.inmemory_cache[query] = (user_data, tweets_df)
        else:
            user_data, tweets_df = cached_result
            # tweets_df = tweets_df.sort_values(by=sort_options_cached[selected_sort][0], ascending=sort_options_cached[selected_sort][1])
            query_time = cache_time

        if isinstance(user_data, str):
            st.write(user_data)
        else:
            st.write("User details:")
            st.json(user_data)
            if not tweets_df.empty:
                st.write(f"Total tweets found: {len(tweets_df)}")
                st.write(f"Time taken to retrieve tweets: {query_time:.2f} seconds")
                st.write("Sample Tweets:")
                st.dataframe(tweets_df.head(num_tweets_to_display))
            else:
                st.write("No tweets found for this user.")


if "results_df" not in st.session_state:
    st.session_state.results_df = pd.DataFrame()

if not st.session_state.results_df.empty:
    selected_indices = st.multiselect(
        "Select tweets to see more from the authors:",
        st.session_state.results_df.index,
        format_func=lambda x: f"User ID {st.session_state.results_df.loc[x, 'user_id']}",
    )
    st.dataframe(st.session_state.results_df.head(num_tweets_to_display))

    if selected_indices:
        if st.button("Show More Tweets from Selected Users"):
            for selected_index in selected_indices:
                user_id = st.session_state.results_df.loc[selected_index, "user_id"]
                cached_result, elapsed_time = check_cache("show_more" + str(user_id))
                if cached_result is None:
                    user_tweets_df = get_tweets_by_user(
                        user_id, bucket_name, scope_name, collection_name
                    )
                    st.session_state.inmemory_cache["show_more" + str(user_id)] = (
                        user_tweets_df
                    )
                else:
                    user_tweets_df = cached_result

                if not user_tweets_df.empty:
                    st.subheader(f"More tweets by user ID {user_id}:")
                    st.dataframe(user_tweets_df)
                else:
                    st.write(f"No additional tweets found for user ID {user_id}.")
else:
    st.session_state.results_df = pd.DataFrame()


if not st.session_state.results_df.empty:
    selected_indices = st.multiselect(
        "Select tweets to see about retweets:",
        st.session_state.results_df.index,
        format_func=lambda x: f"Tweet ID {st.session_state.results_df.loc[x, 'original_tweet_id']}",
    )
    st.dataframe(st.session_state.results_df.head(num_tweets_to_display))

    if selected_indices:
        if st.button("Show Retweet Information of the Selected Tweet"):
            for selected_index in selected_indices:
                tweet_id = st.session_state.results_df.loc[
                    selected_index, "original_tweet_id"
                ]

                cached_result, elapsed_time = check_cache("retweet" + str(tweet_id))
                if cached_result is None:
                    retweets_df = get_retweets_info(tweet_id, inventory_scope)
                    st.session_state.inmemory_cache["retweet" + str(tweet_id)] = (
                        retweets_df
                    )
                else:
                    retweets_df = cached_result

                if not retweets_df.empty:
                    st.subheader(f"Retweet information for Tweet ID {tweet_id}:")
                    st.dataframe(retweets_df)
                else:
                    st.write(f"No retweet information found for Tweet ID {tweet_id}.")
else:
    st.session_state.results_df = pd.DataFrame()


st.title("Dashboard Metrics")

if "redis_cache" not in st.session_state:
    st.session_state.redis_cache = RedisCache()


def get_top_followed_users():
    conn = psycopg2.connect(
        dbname=PGDATABASE, user=PGUSER, password=PGPASSWORD, host=PGHOST, port=PGPORT
    )
    cur = conn.cursor()
    cur.execute("""
        SELECT name, screen_name, followers_count
        FROM users_final
        ORDER BY followers_count DESC
        LIMIT 10;
    """)
    top_users = cur.fetchall()
    cur.close()
    conn.close()
    df_top_users = pd.DataFrame(
        top_users, columns=["Name", "Screen Name", "Followers Count"]
    )
    return df_top_users


with st.expander("Top 10 Most Followed Users", expanded=False):
    try:
        if st.session_state.redis_cache.exists("top_users"):
            cached_result = st.session_state.redis_cache.get("top_users")
            df_top_users = pd.read_json(BytesIO(cached_result))
        else:
            df_top_users = get_top_followed_users()
            st.session_state.redis_cache.set("top_users", df_top_users)
    except Exception as e:
        raise e

    st.columns(3)[1].header("Top 10 Most Followed Users")

    fig_most_followed = px.bar(
        df_top_users,
        y="Screen Name",
        x="Followers Count",
        orientation="h",
        title="Bar Chart: Most Followed Users",
    )
    fig_most_followed.update_layout(
        yaxis={"categoryorder": "total ascending"},
        xaxis_title="Followers Count",
        yaxis_title="Screen Name",
        title={"x": 0.5, "xanchor": "center"},
    )

    col1, col2 = st.columns(2)
    with col1:
        st.dataframe(df_top_users)
    with col2:
        st.plotly_chart(fig_most_followed)


def get_country_code(country_name):
    try:
        return pycountry.countries.lookup(country_name).alpha_3
    except:
        return None


def get_top_creator_locations():
    with psycopg2.connect(
        dbname=PGDATABASE, user=PGUSER, password=PGPASSWORD, host=PGHOST, port=PGPORT
    ) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT location, COUNT(*) AS user_count
                FROM users_final
                WHERE location IS NOT NULL AND location != 'MISSING_INFORMATION' AND location != 'NA'
                GROUP BY location
                ORDER BY user_count DESC
                LIMIT 100;
            """)
            result = cur.fetchall()
            df = pd.DataFrame(result, columns=["Location", "User Count"])
    return df


if st.session_state.redis_cache.exists("top_locations"):
    df_top_locations = pd.read_json(
        BytesIO(st.session_state.redis_cache.get("top_locations"))
    )
else:
    df_top_locations = get_top_creator_locations()
    st.session_state.redis_cache.set("top_locations", df_top_locations)

df_top_locations = df_top_locations.loc[df_top_locations['Location']!="NA"]
df_top_locations["Country Code"] = df_top_locations["Location"].apply(get_country_code)
df_top_locations = df_top_locations[df_top_locations["Country Code"].notnull()]
df_top_locations = df_top_locations.head(10)

with st.expander("Top 10 Locations With Most Creators", expanded=True):
    st.columns(3)[1].header("Top 10 Locations With Most Creators")

    fig = px.choropleth(
        df_top_locations,
        locations="Country Code",
        color="User Count",
        hover_name="Location",
        color_continuous_scale=px.colors.sequential.Plasma,
        projection="natural earth",
    )

    fig.update_layout(
        title="Geographic Distribution of Users",
        title_x=0.325,
        geo=dict(
            showframe=True, showcoastlines=True, projection_type="equirectangular"
        ),
    )

    col1, col2 = st.columns([1, 2])
    with col1:
        st.dataframe(df_top_locations)
    with col2:
        st.plotly_chart(fig, use_container_width=True)


def get_top_hashtags():
    hashtag_query = f"""
    SELECT RAW h
    FROM `{bucket_name}`.`{scope_name}`.`{collection_name}`
    UNNEST SPLIT(SUBSTR(tweets.hashtags, 2, LENGTH(tweets.hashtags) - 2), "', '") AS h
    WHERE tweets.hashtags IS NOT MISSING AND tweets.hashtags != '[]'
    """

    try:
        all_hashtags_results = inventory_scope.query(hashtag_query)
        all_hashtags = [str(row).replace("']","") for row in all_hashtags_results]

        hashtag_counter = Counter(all_hashtags)

        top_hashtags = hashtag_counter.most_common(10)

        df_top_hashtags = pd.DataFrame(top_hashtags, columns=["Hashtag", "Count"])

        return df_top_hashtags

    except Exception as e:
        print("Error during database query:", e)
        return pd.DataFrame()


with st.expander("Top 10 Most Used Hashtags", expanded=False):
    st.columns(3)[1].header("Top 10 Most Used Hashtags")

    if st.session_state.redis_cache.exists("top_hashtags"):
        df_top_hashtags = pd.read_json(
            BytesIO(st.session_state.redis_cache.get("top_hashtags"))
        )
    else:
        df_top_hashtags = get_top_hashtags()
        st.session_state.redis_cache.set("top_hashtags", df_top_hashtags)

    fig_most_used_hashtags = px.bar(
        df_top_hashtags,
        x="Count",
        y="Hashtag",
        orientation="h",
        title="Bar Chart: Most Used Hashtags",
    )

    fig_most_used_hashtags.update_layout(
        yaxis={"categoryorder": "total ascending"},
        xaxis_title="Count",
        yaxis_title="Hashtag",
        title={"x": 0.5, "xanchor": "center"},
    )

    col1, col2 = st.columns(2)
    with col1:
        st.dataframe(df_top_hashtags)
    with col2:
        st.plotly_chart(fig_most_used_hashtags, use_container_width=True)


def get_top_retweeted_tweets():
    tweet_query = f"""
    SELECT original_tweet_id, COUNT(*) as retweet_count
    FROM `{bucket_name}`.`{scope_name}`.`{collection_name}`
    WHERE is_retweet = 'True'
    GROUP BY original_tweet_id
    ORDER BY retweet_count DESC
    LIMIT 10;
    """
    tweet_results = inventory_scope.query(tweet_query)
    df_top_tweets = pd.DataFrame(tweet_results)
    return df_top_tweets


with st.expander("Top 10 Retweeted Tweets", expanded=False):
    if st.session_state.redis_cache.exists("top_tweets"):
        df_top_tweets = pd.read_json(
            BytesIO(st.session_state.redis_cache.get("top_tweets"))
        )
    else:
        df_top_tweets = get_top_retweeted_tweets()
        st.session_state.redis_cache.set("top_tweets", df_top_tweets)

    st.columns(3)[1].header("Top 10 Most Retweeted Tweets")

    fig = px.pie(df_top_tweets, values="retweet_count", names="original_tweet_id")

    col1, col2 = st.columns(2)
    with col1:
        st.dataframe(df_top_tweets.style.format({"retweet_count": "{:,}"}))
    with col2:
        st.plotly_chart(fig, use_container_width=True)
