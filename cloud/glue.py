mport numpy as np
from datetime import datetime
import matplotlib.pyplot as plt
import seaborn as sns

class MusicStreamingProcessor:
    """
    Data processing pipeline for music streaming data
    """
    
    def __init__(self, songs_path, streams_path, users_path):
        """Initialize with paths to data files"""
        self.songs_path = songs_path
        self.streams_path = streams_path
        self.users_path = users_path
        
        # Will store processed dataframes
        self.songs_df = None
        self.streams_df = None
        self.users_df = None
        self.integrated_data = None
        
        # Analytics dataframes
        self.track_stats = None
        self.user_stats = None
        self.genre_stats = None
        self.country_stats = None
        
    def load_data(self):
        """Load raw data from CSV files"""
        self.songs_df = pd.read_csv(self.songs_path)
        self.streams_df = pd.read_csv(self.streams_path)
        self.users_df = pd.read_csv(self.users_path)
        
        print(f"Loaded songs data: {self.songs_df.shape[0]} tracks")
        print(f"Loaded streams data: {self.streams_df.shape[0]} stream records")
        print(f"Loaded users data: {self.users_df.shape[0]} users")
        
        return self
        
    def explore_data(self):
        """Perform initial data exploration"""
        # Check for missing values
        missing_data = {
            'songs': self.songs_df.isnull().sum(),
            'streams': self.streams_df.isnull().sum(),
            'users': self.users_df.isnull().sum()
        }
        
        # Check for duplicates
        duplicates = {
            'songs': self.songs_df.duplicated().sum(),
            'streams': self.streams_df.duplicated().sum(),
            'users': self.users_df.duplicated().sum()
        }
        
        print("\n=== Missing Values ===")
        for dataset, missing in missing_data.items():
            if missing.sum() > 0:
                print(f"\n{dataset.capitalize()} dataset missing values:")
                print(missing[missing > 0])
            else:
                print(f"\n{dataset.capitalize()} dataset: No missing values")
                
        print("\n=== Duplicate Rows ===")
        for dataset, dupe_count in duplicates.items():
            print(f"{dataset.capitalize()} dataset: {dupe_count} duplicates")
            
        # Basic statistics for audio features
        audio_features = ['danceability', 'energy', 'loudness', 'speechiness', 
                         'acousticness', 'instrumentalness', 'liveness', 'valence', 'tempo']
        
        print("\n=== Audio Features Statistics ===")
        print(self.songs_df[audio_features].describe().T)
        
        return self
    
    def clean_data(self):
        """Clean and preprocess all datasets"""
        # Make copies to avoid modifying original data
        songs_clean = self.songs_df.copy()
        streams_clean = self.streams_df.copy()
        users_clean = self.users_df.copy()
        
        # Clean songs data
        # Fill missing numeric values with median
        numeric_cols = songs_clean.select_dtypes(include=['float64', 'int64']).columns
        for col in numeric_cols:
            songs_clean[col] = songs_clean[col].fillna(songs_clean[col].median())
            
        # Fill missing categorical values with 'unknown'
        categorical_cols = songs_clean.select_dtypes(include=['object']).columns
        for col in categorical_cols:
            songs_clean[col] = songs_clean[col].fillna('unknown')
            
        # Handle extreme values in duration
        songs_clean['duration_ms'] = songs_clean['duration_ms'].clip(
            lower=30000,    # 30 seconds
            upper=600000    # 10 minutes
        )
        
        # Clean users data
        # Convert timestamp to datetime
        users_clean['created_at'] = pd.to_datetime(users_clean['created_at'])
        
        # Ensure country codes are uppercase
        if 'user_country' in users_clean.columns:
            users_clean['user_country'] = users_clean['user_country'].str.upper()
            
        # Handle age outliers or invalid values
        users_clean['user_age'] = users_clean['user_age'].clip(lower=13, upper=100)
        
        # Ensure consistent data types for joining
        streams_clean['user_id'] = streams_clean['user_id'].astype(str)
        users_clean['user_id'] = users_clean['user_id'].astype(str)
        
        # Update the dataframes
        self.songs_df = songs_clean
        self.streams_df = streams_clean
        self.users_df = users_clean
        
        print("Data cleaning completed")
        return self
    
    def engineer_features(self):
        """Create derived features to enhance analysis"""
        # Songs features
        self.songs_df['duration_min'] = self.songs_df['duration_ms'] / 60000
        
        # Popularity categories
        self.songs_df['popularity_category'] = pd.cut(
            self.songs_df['popularity'],
            bins=[0, 25, 50, 75, 100],
            labels=['Low', 'Medium', 'High', 'Very High']
        )
        
        # Extract artist count
        self.songs_df['artist_count'] = self.songs_df['artists'].apply(
            lambda x: len(str(x).split(',')) if pd.notna(x) else 0
        )
        
        # User features
        # Age groups
        self.users_df['age_group'] = pd.cut(
            self.users_df['user_age'],
            bins=[0, 18, 25, 35, 50, 100],
            labels=['Under 18', '18-24', '25-34', '35-49', '50+']
        )
        
        # Account age
        self.users_df['account_age_days'] = (
            pd.Timestamp.now() - self.users_df['created_at']
        ).dt.days
        
        print("Feature engineering completed")
        return self
    
    def integrate_data(self):
        """Join datasets to create comprehensive view"""
        # Merge streams with song information
        streams_with_songs = pd.merge(
            self.streams_df,
            self.songs_df[['track_id', 'artists', 'track_name', 'track_genre', 
                          'popularity', 'duration_ms', 'popularity_category']],
            on='track_id',
            how='left'
        )
        
        # Add user information
        full_streams_data = pd.merge(
            streams_with_songs,
            self.users_df[['user_id', 'user_name', 'user_age', 'user_country', 'age_group']],
            on='user_id',
            how='left'
        )
        
        # Calculate listen percentage
        full_streams_data['listen_percentage'] = (
            full_streams_data['listen_time'] / full_streams_data['duration_ms'] * 100
        ).clip(upper=100)  # Cap at 100%
        
        # Flag complete listens (>85% of song listened)
        full_streams_data['complete_listen'] = full_streams_data['listen_percentage'] > 85
        
        self.integrated_data = full_streams_data
        print(f"Data integration complete. Integrated dataset has {full_streams_data.shape[0]} records and {full_streams_data.shape[1]} columns")
        
        return self
    
    def compute_analytics(self):
        """Generate analytics from the integrated data"""
        # Ensure data is integrated
        if self.integrated_data is None:
            print("Error: Must run integrate_data() before computing analytics")
            return self
            
        # Track-level analytics
        self.track_stats = self.integrated_data.groupby('track_id').agg(
            track_name=('track_name', 'first'),
            artists=('artists', 'first'),
            genre=('track_genre', 'first'),
            stream_count=('user_id', 'count'),
            unique_listeners=('user_id', 'nunique'),
            avg_listen_percentage=('listen_percentage', 'mean'),
            complete_listen_rate=('complete_listen', 'mean'),
            countries_reached=('user_country', 'nunique')
        ).reset_index()
        
        # Add popularity metrics
        self.track_stats['engagement_score'] = (
            self.track_stats['avg_listen_percentage'] * 
            self.track_stats['complete_listen_rate'] * 
            np.log1p(self.track_stats['stream_count'])
        )
        
        # Genre-level analytics
        self.genre_stats = self.integrated_data.groupby('track_genre').agg(
            total_streams=('user_id', 'count'),
            unique_tracks=('track_id', 'nunique'),
            unique_listeners=('user_id', 'nunique'),
            avg_popularity=('popularity', 'mean'),
            avg_listen_percentage=('listen_percentage', 'mean'),
            complete_listen_rate=('complete_listen', 'mean')
        ).reset_index()
        
        # User listening patterns
        self.user_stats = self.integrated_data.groupby('user_id').agg(
            user_name=('user_name', 'first'),
            age=('user_age', 'first'),
            age_group=('age_group', 'first'),
            country=('user_country', 'first'),
            stream_count=('track_id', 'count'),
            unique_tracks=('track_id', 'nunique'),
            unique_genres=('track_genre', 'nunique'),
            avg_listen_percentage=('listen_percentage', 'mean'),
            complete_listen_rate=('complete_listen', 'mean')
        ).reset_index()
        
        # Calculate favorite genre for each user
        user_genre_counts = self.integrated_data.groupby(['user_id', 'track_genre']).size().reset_index(name='count')
        top_genres = user_genre_counts.sort_values(['user_id', 'count'], ascending=[True, False]).drop_duplicates('user_id')
        self.user_stats = pd.merge(self.user_stats, top_genres[['user_id', 'track_genre']], on='user_id', how='left')
        self.user_stats.rename(columns={'track_genre': 'favorite_genre'}, inplace=True)
        
        # Add genre diversity metric
        self.user_stats['genre_diversity'] = self.user_stats['unique_genres'] / self.user_stats['stream_count']
        self.user_stats['genre_diversity'] = self.user_stats['genre_diversity'].fillna(0)
        
        # Country analytics
        self.country_stats = self.integrated_data.groupby('user_country').agg(
            user_count=('user_id', 'nunique'),
            total_streams=('track_id', 'count'),
            avg_streams_per_user=('track_id', 'count') / ('user_id', 'nunique'),
            avg_listen_percentage=('listen_percentage', 'mean')
        ).reset_index()
        
        # Add top genre per country
        country_genre_counts = self.integrated_data.groupby(['user_country', 'track_genre']).size().reset_index(name='count')
        top_country_genres = country_genre_counts.sort_values(['user_country', 'count'], ascending=[True, False]).drop_duplicates('user_country')
        self.country_stats = pd.merge(self.country_stats, top_country_genres[['user_country', 'track_genre']], on='user_country', how='left')
        self.country_stats.rename(columns={'track_genre': 'top_genre'}, inplace=True)
        
        print("Analytics computation complete")
        return self
    
    def generate_insights(self):
        """Identify key insights from the analytics"""
        insights = []
        
        # Ensure analytics are computed
        if self.track_stats is None or self.genre_stats is None or self.user_stats is None:
            print("Error: Must run compute_analytics() before generating insights")
            return []
        
        # Top performing tracks
        top_tracks = self.track_stats.sort_values('engagement_score', ascending=False).head(10)
        insights.append({
            'category': 'Top Performing Tracks',
            'insight': f"The top performing track is '{top_tracks.iloc[0]['track_name']}' by {top_tracks.iloc[0]['artists']} with {top_tracks.iloc[0]['stream_count']} streams and {top_tracks.iloc[0]['unique_listeners']} unique listeners."
        })
        
        # Most popular genres
        top_genres = self.genre_stats.sort_values('total_streams', ascending=False).head(5)
        insights.append({
            'category': 'Popular Genres',
            'insight': f"The most streamed genre is '{top_genres.iloc[0]['track_genre']}' with {top_genres.iloc[0]['total_streams']} streams across {top_genres.iloc[0]['unique_tracks']} tracks."
        })
        
        # User engagement
        avg_listen_pct = self.integrated_data['listen_percentage'].mean()
        complete_listen_rate = self.integrated_data['complete_listen'].mean() * 100
        insights.append({
            'category': 'User Engagement',
            'insight': f"Users listen to {avg_listen_pct:.1f}% of tracks on average, with {complete_listen_rate:.1f}% of streams being complete listens (>85% of track)."
        })
        
        # Age group preferences
        age_group_genres = self.integrated_data.groupby(['age_group', 'track_genre']).size().reset_index(name='count')
        age_group_top_genres = age_group_genres.sort_values(['age_group', 'count'], ascending=[True, False]).drop_duplicates('age_group')
        
        for _, row in age_group_top_genres.iterrows():
            if pd.notna(row['age_group']):
                insights.append({
                    'category': 'Age Demographics',
                    'insight': f"The '{row['age_group']}' age group prefers the '{row['track_genre']}' genre."
                })
        
        # Geographic insights
        if self.country_stats is not None and len(self.country_stats) > 0:
            top_country = self.country_stats.sort_values('total_streams', ascending=False).iloc[0]
            insights.append({
                'category': 'Geographic Trends',
                'insight': f"Users from {top_country['user_country']} are the most active with {top_country['total_streams']} streams, averaging {top_country['avg_streams_per_user']:.1f} streams per user."
            })
        
        print(f"Generated {len(insights)} key insights")
        return insights
        
    def process_pipeline(self):
        """Run the full data processing pipeline"""
        return (self
                .load_data()
                .explore_data()
                .clean_data()
                .engineer_features()
                .integrate_data()
                .compute_analytics())
    
    def export_processed_data(self, output_dir):
        """Export processed datasets to CSV files"""
        import os
        os.makedirs(output_dir, exist_ok=True)
        
        # Export cleaned datasets
        self.songs_df.to_csv(f"{output_dir}/songs_processed.csv", index=False)
        self.users_df.to_csv(f"{output_dir}/users_processed.csv", index=False)
        
        # Export analytics
        if self.track_stats is not None:
            self.track_stats.to_csv(f"{output_dir}/track_analytics.csv", index=False)
        if self.genre_stats is not None:
            self.genre_stats.to_csv(f"{output_dir}/genre_analytics.csv", index=False)
        if self.user_stats is not None:
            self.user_stats.to_csv(f"{output_dir}/user_analytics.csv", index=False)
        if self.country_stats is not None:
            self.country_stats.to_csv(f"{output_dir}/country_analytics.csv", index=False)
            
        print(f"Data exported to {output_dir}")
        return self
    

        
