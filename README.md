# Big Data Learning with PySpark

A comprehensive learning environment for big data processing using PySpark with live UK government data sources.

## 🎯 Features

- **PySpark** - Distributed data processing framework
- **Live UK Data** - Real-time traffic and transport data from Transport for London (TfL)
- **GPS Data Processing** - Sample GPS data analysis pipeline
- **Jupyter Lab** - Interactive notebooks for exploration
- **Docker** - Fully containerized and reproducible environment
- **Local Spark Execution** - No cloud dependencies (AWS/Bedrock)

## 📋 Prerequisites

- Docker & Docker Compose
- OR Python 3.11+ (for local setup without Docker)
- Java 17+ (required for local PySpark execution)
- 4GB+ RAM recommended

## 🚀 Quick Start

### Option 1: Using Docker (Recommended)

```bash
# Build the Docker image
docker-compose build

# Start Jupyter Lab with PySpark
docker-compose up -d jupyter

# View the Jupyter token
docker logs bigdata-jupyter

# Access Jupyter Lab
# Go to http://localhost:8888
# Enter the token from the logs
```

### Option 2: Local Setup

```bash
# Navigate to bigdata directory
cd d:\python\bigdata

# Activate virtual environment
.\.venv\Scripts\activate

# Verify Java is installed (required by PySpark)
java -version

# Install dependencies
pip install -r requirements.txt

# Run the processor
python src/spark_processor.py

# Or start Jupyter Lab
jupyter lab
```

## 📊 Data Sources

### 1. Transport for London (TfL) API
- **Traffic Incidents**: Real-time disruptions across London
- **Line Status**: Underground line operational status
- **No authentication required**
- **URL**: https://api.tfl.gov.uk

### 2. Sample GPS Data
- Synthetic GPS records for demonstration
- Includes timestamp, latitude, longitude, speed, vehicle ID
- Useful for learning PySpark transformations and aggregations

## 🔍 Project Structure

```
bigdata/
├── .venv/                 # Python virtual environment
├── src/
│   ├── data_fetcher.py   # Fetch UK gov data
│   └── spark_processor.py # PySpark pipeline
├── notebooks/            # Jupyter notebooks
├── data/                 # Local data storage
├── docker-compose.yml    # Docker orchestration
├── Dockerfile           # Container definition
├── requirements.txt     # Python dependencies
└── README.md           # This file
```

## 📝 Usage Examples

### Running the Spark Processor

```bash
# Local execution
python src/spark_processor.py

# Docker execution
docker-compose run spark-processor
```

### Using Jupyter Lab

1. Open http://localhost:8888 in your browser
2. Enter the token from the logs
3. Navigate to `notebooks/` folder
4. Create new notebooks or run existing ones

### Sample PySpark Code

```python
from pyspark.sql import SparkSession
from src.data_fetcher import generate_sample_gps_data

# Initialize Spark
spark = SparkSession.builder \
    .appName("BigDataLearning") \
    .master("local[*]") \
    .getOrCreate()

# Load GPS data
pdf = generate_sample_gps_data(10000)
gps_df = spark.createDataFrame(pdf)

# Analyze
gps_df.groupBy('location').agg({'speed_kmh': 'avg'}).show()
```

## 🐳 Docker Commands

```bash
# View running containers
docker-compose ps

# View logs
docker-compose logs -f jupyter

# Stop all containers
docker-compose down

# Stop and remove volumes
docker-compose down -v

# Access container shell
docker-compose exec jupyter bash
```

## 📚 Learning Path

1. **Basics**: Understand Spark DataFrames and RDDs
2. **Data Loading**: Learn to fetch and load data from APIs
3. **Transformations**: Apply map, filter, groupBy operations
4. **Aggregations**: Use SQL functions for data summarization
5. **Visualization**: Create insights from processed data
6. **Real-time**: Implement streaming data pipelines

## 🔧 Configuration

### Spark Configuration
- **Master**: local[*] (use all available cores)
- **Shuffle Partitions**: 4 (adjust based on data size)
- **Memory**: Default Java heap size (suitable for learning)

### To adjust Spark settings:

Edit `src/spark_processor.py`:
```python
.config("spark.driver.memory", "2g") \
.config("spark.sql.shuffle.partitions", "8") \
```

## ⚠️ Troubleshooting

### Jupyter token not shown
```bash
docker-compose logs jupyter | grep token
```

### PySpark not found
```bash
# Reinstall PySpark
pip install --upgrade pyspark
```

### Memory issues
- Reduce sample data size in `data_fetcher.py`
- Increase Docker memory allocation
- Adjust Spark config

### Port already in use
```bash
# Change port in docker-compose.yml
ports:
  - "8889:8888"  # Changed from 8888 to 8889
```

## 📖 Additional Resources

- [PySpark Docs](https://spark.apache.org/docs/latest/api/python/)
- [TfL API Documentation](https://tfl.gov.uk/info-for/open-data-users/)
- [UK Gov Open Data](https://www.data.gov.uk/)
- [Apache Spark SQL Guide](https://spark.apache.org/docs/latest/sql-programming-guide.html)

## 🤝 Next Steps

1. **Add more data sources**: Integrate other UK gov APIs
2. **Implement streaming**: Use Spark Structured Streaming
3. **Scale up**: Deploy to a Spark cluster
4. **Visualize**: Add Plotly/Matplotlib for insights
5. **Real-time dashboards**: Integrate with tools like Elasticsearch

## 💡 Tips

- Start with small datasets to understand transformations
- Use `explain()` to understand Spark execution plans
- Monitor Spark UI at http://localhost:4040 during execution
- Partition data wisely for distributed processing
- Cache frequently used DataFrames for performance

## 🎓 Local Development Setup

No AWS/Cloud dependencies required - everything runs locally!

- Spark runs in local mode
- Data is fetched from public APIs (no authentication needed for basic TfL data)
- All processing happens on your machine
- Docker ensures reproducible environment

---

Happy Learning! 🚀
