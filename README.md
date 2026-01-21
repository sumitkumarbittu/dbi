# DBI - Database File Injector

[![Python](https://img.shields.io/badge/Python-3.11+-blue.svg)](https://python.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.110+-green.svg)](https://fastapi.tiangolo.com)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-13+-blue.svg)](https://postgresql.org)
[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

## 🚀 **Why DBI?**

- 🎯 **Zero-Config Data Import** - Upload CSV/JSON files directly to PostgreSQL with automatic schema detection
- ⚡ **Lightning Fast** - Async processing with PostgreSQL's native `COPY` command for bulk data ingestion
- 🛡️ **Production Ready** - Built-in conflict resolution, error handling, and comprehensive validation
- 🔄 **Smart Deduplication** - Automatic primary key conflict handling with `ON CONFLICT DO NOTHING`
- 🎨 **Modern UI** - Clean web interface plus complete RESTful API for automation
- 🐳 **Deploy Anywhere** - Docker containerized with health checks and cloud deployment ready
- 📊 **Real-time Monitoring** - Background job processing with live status tracking and detailed logging

## 🎯 Overview

**DBI (Database File Injector)** is a robust, production-ready web service designed to streamline the process of importing data from CSV and JSON files into PostgreSQL databases. Built with modern Python technologies including FastAPI and async PostgreSQL drivers, DBI provides a comprehensive solution for data ingestion with enterprise-grade features such as intelligent conflict resolution, background job processing, and real-time status tracking.

The service addresses common data import challenges by offering automatic primary key deduplication, support for PostgreSQL identity columns, schema validation, and a clean web interface for both technical and non-technical users. Whether you're performing one-time data migrations or setting up recurring data pipelines, DBI provides the reliability and flexibility needed for production environments.

## ✨ Key Features

### 🔄 **Intelligent Data Import**
- **Multi-Format Support**: Seamlessly handle both CSV files with headers and JSON arrays of objects
- **Smart Column Mapping**: Automatic validation and mapping of file columns to database table schemas
- **Flexible Primary Key Handling**: Optional primary key specification with automatic conflict detection and resolution
- **Identity Column Awareness**: Proper handling of PostgreSQL `IDENTITY ALWAYS` columns with `OVERRIDING SYSTEM VALUE` when needed

### ⚡ **High-Performance Processing**
- **Asynchronous Architecture**: Built on FastAPI with async PostgreSQL operations for optimal performance
- **Background Job Queue**: Non-blocking file uploads with dedicated background processing
- **Efficient Memory Usage**: Streaming CSV processing using PostgreSQL's native `COPY` command
- **Batch Operations**: Optimized bulk inserts for JSON data with proper transaction management

### 🛡️ **Enterprise-Grade Reliability**
- **Conflict Resolution**: Automatic deduplication using `ON CONFLICT DO NOTHING` to prevent duplicate entries
- **Comprehensive Validation**: SQL identifier validation, schema verification, and file format checking
- **Error Handling**: Detailed error reporting with proper HTTP status codes and descriptive messages
- **Transaction Safety**: Proper transaction management with rollback on failures

### 🎨 **Modern User Experience**
- **Responsive Web Interface**: Clean, intuitive web UI for database operations and file uploads
- **Real-time Job Tracking**: Live status updates for import jobs with progress indicators
- **RESTful API**: Complete API coverage for programmatic integration and automation
- **Interactive Schema Discovery**: Dynamic table schema inspection and column suggestions

### 🐳 **Production Deployment Ready**
- **Docker Containerization**: Optimized multi-stage Docker builds with health checks
- **Environment Configuration**: Flexible configuration through environment variables
- **Monitoring Support**: Built-in health endpoints and structured logging
- **Cloud-Native**: Pre-configured deployment manifests for Render.com and other cloud platforms

### 🔒 **Security & Compliance**
- **SQL Injection Prevention**: Comprehensive identifier validation and parameterized queries
- **File Type Restrictions**: Secure file upload handling with format validation
- **CORS Configuration**: Configurable cross-origin resource sharing for web applications
- **Connection Security**: Secure PostgreSQL connection handling with proper cleanup

### 📊 **Observability & Debugging**
- **Comprehensive Logging**: Detailed request/response logging with configurable log levels
- **Job Monitoring**: Real-time job status tracking with detailed error reporting
- **Health Checks**: Built-in health endpoints for load balancer and monitoring integration
- **Performance Metrics**: Job processing statistics and throughput monitoring

## 🚀 Features

- **Multi-format Support**: Import both CSV and JSON files
- **Intelligent Deduplication**: Automatic handling of primary key conflicts with `ON CONFLICT DO NOTHING`
- **Background Processing**: Non-blocking file uploads with job tracking
- **Identity Column Support**: Handles PostgreSQL identity columns correctly
- **Modern Web UI**: Clean, responsive interface for database operations
- **API-First Design**: RESTful API for programmatic access
- **Docker Ready**: Containerized deployment with health checks
- **Production Optimized**: Structured logging, error handling, and monitoring

## 📋 Prerequisites

- Python 3.11+
- PostgreSQL 13+
- Docker (optional, for containerized deployment)

## 🛠️ Installation

### Local Development

1. **Clone the repository**
   ```bash
   git clone https://github.com/your-username/dbi.git
   cd dbi
   ```

2. **Create a virtual environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Set up environment variables**
   ```bash
   export DATABASE_URL="postgresql://username:password@localhost:5432/dbname"
   export PORT=8000
   ```

5. **Run the application**
   ```bash
   python app.py
   ```

### Docker Deployment

1. **Build the image**
   ```bash
   docker build -t dbi .
   ```

2. **Run the container**
   ```bash
   docker run -p 8000:8000 \
     -e DATABASE_URL="postgresql://username:password@host:5432/dbname" \
     -e PORT=8000 \
     dbi
   ```

## 🌐 Deployment

### Render.com

The project includes a pre-configured `render.yaml` for easy deployment on Render:

```bash
# Connect your repository to Render
# The service will automatically deploy using the render.yaml configuration
```

### Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| `DATABASE_URL` | PostgreSQL connection string | Yes |
| `PORT` | Application port (default: 8000) | No |

## 📡 API Documentation

### Base URL
```
http://localhost:8000
```

### Endpoints

#### Database Configuration
```http
POST /save-db
Content-Type: application/json

{
  "database_url": "postgresql://user:pass@host:5432/dbname"
}
```

#### File Upload
```http
POST /upload-data
Content-Type: multipart/form-data

table: your_table_name
columns: col1,col2,col3
primary_key: id
file: [CSV or JSON file]
```

#### Table Schema
```http
GET /table-schema?table=your_table&schema=public
```

#### Job Status
```http
GET /job-status/{job_id}
```

#### Running Jobs
```http
GET /jobs/running
```

#### Create Table
```http
POST /create-table
Content-Type: application/json

{
  "create_sql": "CREATE TABLE users (id SERIAL PRIMARY KEY, name VARCHAR(100))"
}
```

#### Health Check
```http
GET /health
```

## 💡 Usage Examples

### CSV Import with Primary Key Deduplication

1. **Configure database connection**
   ```bash
   curl -X POST http://localhost:8000/save-db \
     -H "Content-Type: application/json" \
     -d '{"database_url": "postgresql://user:pass@localhost:5432/db"}'
   ```

2. **Upload CSV file**
   ```bash
   curl -X POST http://localhost:8000/upload-data \
     -F "table=users" \
     -F "columns=id,name,email" \
     -F "primary_key=id" \
     -F "file=@users.csv"
   ```

3. **Check job status**
   ```bash
   curl http://localhost:8000/job-status/1234
   ```

### JSON Import

```bash
curl -X POST http://localhost:8000/upload-data \
  -F "table=products" \
  -F "columns=id,name,price" \
  -F "primary_key=id" \
  -F "file=@products.json"
```

## 🏗️ Architecture

### Core Components

- **FastAPI Backend**: High-performance async web framework
- **PostgreSQL Client**: Async psycopg3 for database operations
- **Background Tasks**: Non-blocking file processing with job tracking
- **Web Interface**: Modern HTML/CSS/JS frontend
- **Docker Container**: Production-ready containerization

### Data Flow

1. Client uploads file via web interface or API
2. Server validates file format and table schema
3. Background job is created for processing
4. File data is imported with conflict resolution
5. Job status is updated and available for polling

## 🔧 Configuration

### Database Permissions

The application requires the following PostgreSQL permissions:
- `SELECT` on `information_schema` (for schema introspection)
- `INSERT` on target tables
- `CREATE` on schema (for temporary tables)

### Supported Data Types

- **CSV**: Header-based column mapping
- **JSON**: Array of objects with matching keys

### Identity Columns

The service automatically handles PostgreSQL identity columns:
- `IDENTITY ALWAYS` columns are properly managed
- `OVERRIDING SYSTEM VALUE` is used when needed

## 🧪 Testing

### Running Tests

```bash
# Install test dependencies
pip install pytest pytest-asyncio httpx

# Run tests
pytest tests/
```

### Test Coverage

- API endpoint testing
- Database operation validation
- File format handling
- Error scenarios

## 📊 Monitoring

### Health Checks

The service provides a `/health` endpoint for monitoring:
```json
{
  "status": "ok"
}
```

### Logging

Logs are written to `/app/logs/app.log` in Docker containers and include:
- Request/response logging
- Error details and stack traces
- Job processing status

## 🔒 Security Considerations

- **SQL Injection Prevention**: All identifiers are validated against regex patterns
- **File Upload Security**: File types are restricted to CSV and JSON
- **CORS Configuration**: Configurable for production environments
- **Database Connection**: Uses connection pooling and proper cleanup

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🆘 Troubleshooting

### Common Issues

**Database Connection Failed**
- Verify `DATABASE_URL` format and credentials
- Check network connectivity to PostgreSQL server
- Ensure database exists and user has permissions

**File Upload Errors**
- Verify file format (CSV or JSON only)
- Check column names match table schema
- Ensure primary key columns are included when specified

**Job Processing Failures**
- Check job status endpoint for error details
- Verify table exists and schema matches
- Review database permissions

### Debug Mode

Enable debug logging:
```bash
export LOG_LEVEL=debug
python app.py
```

## 📞 Support

For questions and support:
- Create an issue on GitHub
- Check the API documentation above
- Review the troubleshooting section

