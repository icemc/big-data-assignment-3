import os
import pandas as pd
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
import numpy as np
import glob

def visualize_clusters(k_value):
    """
    Visualize the clusters for a specific k value using a 3D scatter plot
    """
    # Find all CSV files in the directory with the right pattern
    files = glob.glob(f"visualization_k{k_value}/*.csv")

    if not files:
        print(f"No data files found for k={k_value}")
        return

    # Read and combine all CSV files
    dfs = []
    for file in files:
        if os.path.getsize(file) > 0:  # Skip empty files
            df = pd.read_csv(file)
            dfs.append(df)

    if not dfs:
        print(f"No valid data found for k={k_value}")
        return

    # Combine all dataframes
    data = pd.concat(dfs, ignore_index=True)

    # Create a 3D scatter plot
    fig = plt.figure(figsize=(12, 10))
    ax = fig.add_subplot(111, projection='3d')

    # Colors for different clusters
    colors = plt.cm.tab10(np.linspace(0, 1, k_value))

    # Plot each cluster
    for i in range(k_value):
        cluster_data = data[data['cluster'] == i]
        ax.scatter(
            cluster_data['sensor1'],
            cluster_data['sensor2'],
            cluster_data['sensor3'],
            c=[colors[i]],
            s=cluster_data['distance'] * 100,  # Size points by distance (outliers will be larger)
            label=f'Cluster {i}'
        )

    # Highlight top outliers with red stars
    top_outliers = data.nlargest(25, 'distance')
    ax.scatter(
        top_outliers['sensor1'],
        top_outliers['sensor2'],
        top_outliers['sensor3'],
        c='red',
        marker='*',
        s=200,
        label='Top 25 Outliers'
    )

    ax.set_xlabel('Sensor 1')
    ax.set_ylabel('Sensor 2')
    ax.set_zlabel('Sensor 3')
    ax.set_title(f'Gearbox Readings Clusters (k={k_value})')
    ax.legend()

    # Save the figure
    plt.savefig(f'gearbox_clusters_k{k_value}.png', dpi=300, bbox_inches='tight')
    plt.close()

    print(f"Visualization for k={k_value} saved as gearbox_clusters_k{k_value}.png")

# Create visualizations for each k value from 2 to 12
for k in range(2, 13):
    visualize_clusters(k)

# Create another visualization showing just the top outliers across all k values
def visualize_outliers_comparison():
    """
    Visualize the top outliers for each k value
    """
    plt.figure(figsize=(15, 10))

    # For each k value, plot the average distance and number of outliers
    k_values = range(2, 13)
    avg_distances = []

    for k in k_values:
        # Find all CSV files in the directory with the right pattern
        files = glob.glob(f"visualization_k{k}/*.csv")

        if not files:
            continue

        # Read and combine all CSV files
        dfs = []
        for file in files:
            if os.path.getsize(file) > 0:  # Skip empty files
                df = pd.read_csv(file)
                dfs.append(df)

        if not dfs:
            continue

        # Combine all dataframes
        data = pd.concat(dfs, ignore_index=True)

        # Calculate average distance for this k
        avg_distance = data['distance'].mean()
        avg_distances.append(avg_distance)

    # Plot the average distance vs. k
    plt.plot(k_values, avg_distances, 'o-', linewidth=2)
    plt.grid(True)
    plt.xlabel('Number of Clusters (k)')
    plt.ylabel('Average Distance to Nearest Centroid')
    plt.title('Average Distance vs. Number of Clusters')
    plt.xticks(k_values)

    # Save the figure
    plt.savefig('gearbox_avg_distance_vs_k.png', dpi=300, bbox_inches='tight')
    plt.close()

    print("Comparison of average distances saved as gearbox_avg_distance_vs_k.png")

# Create the outliers comparison visualization
visualize_outliers_comparison()