# Bidirectional ClickHouse ↔ FlatFile Ingestion Tool

A full-stack ingestion system built with **React** and **Spring Boot (Java)** that enables uploading and downloading CSV data from a **ClickHouse** database. Designed for high-throughput, large-scale data pipelines, the tool offers real-time feedback, progress tracking, streaming support, and dynamic column and table control.

---

## 📌 Note

Development work done **before `17th April`** is available on the `master` branch.  
If you prefer to view only the work completed **up to `16th April`**, please switch to the dedicated branch:

```bash
git checkout master
```

> This branch preserves the state of the project *before any updates made on or after 17th April. In frontend please checkout to this log `7706f0251893f2b3231dc6c0f017cfb2aedc32b3`*

---

## 🚀 Features

- 🔁 **Bidirectional Ingestion**: Upload files to ClickHouse & export data from ClickHouse to CSV.
- 📤 **Streamed Uploads**: Real-time multipart CSV ingestion with progress tracking (MB-wise).
- 📥 **Streamed Downloads**: Data exported directly from ClickHouse using `StreamingResponseBody`.
- 📊 **Column Selection**: Choose specific columns to upload or export.
- 🔐 **Secure Auth**: Supports password or JWT-based authentication to ClickHouse.
- 📄 **Preview Mode**: View first 100 rows before downloading or uploading data.
- ⚠️ **Error-Handled**: All ClickHouse and network exceptions handled gracefully.
- ✅ **Progress Bars**: Real-time upload/download progress displayed in MB and %.

---

## ⚙️ Tech Stack

- **Frontend**: React (with JSX), Axios
- **Backend**: Java, Spring Boot (REST + Streaming)
- **Database**: ClickHouse (25.x or later)
- **Others**: Multipart handling, `InputStream`/`OutputStream`, CSV parser

---

## 📤 Uploading a CSV to ClickHouse

1. **Go to Upload Section** on the frontend.
2. Select whether to create a **new table** or use an **existing one**.
3. Upload a CSV file using file picker.
4. Provide ClickHouse connection details:
   - Host, Port, Username, Password or JWT Token
   - Target database and table name
5. Click **Upload**:
   - File is streamed to backend in chunks.
   - Progress bar shows real-time upload MB.
   - Backend inserts data into ClickHouse.
6. On success, the data is immediately available in ClickHouse.

---

## 📥 Downloading from ClickHouse

1. **Go to Download Section**.
2. Enter ClickHouse details (host, credentials, etc.).
3. Click **Load Tables**.
4. Select a table and fetch columns.
5. Choose specific columns (or all).
6. Click **Download**:
   - File is streamed from ClickHouse to browser.
   - Progress bar shows MB downloaded.
   - File is saved as `.csv`.

---

## 🖥️ Setup Instructions

### 🔁 Clone the Repository

```bash
git clone --branch postDevelop --single-branch https://github.com/Kendo007/bidirectional.git
git clone https://github.com/Kendo007/bidirectional-frontend
```

---

### 🧱 Backend Setup (Spring Boot + Java)

1. **Open in IntelliJ or VS Code with Java support**.
2. **Configure ClickHouse properties** in `application.yml` (optional).
3. **Run the Spring Boot App**:

```bash
cd bidirectional
mvn spring-boot:run
```

By default, it runs at: `http://localhost:8080`

---

### 🎨 Frontend Setup (React)

1. Navigate to `frontend/` folder:
```bash
cd cd bidirectional-frontend
```

2. Install dependencies:
```bash
npm install
```

3. Run development server:
```bash
npm run start
```

Access the frontend at: `http://localhost:3000`

---

## 🧠 Advanced Features

- **Custom Delimiters**: Upload and download using user-defined delimiters.
- **Header Validation**: Ensures column alignment before ClickHouse ingestion.
- **Authentication Handling**: Invalid credentials show clear UI messages.
- **Download & Upload Byte Counter**: Real-time MB tracked in frontend.
- **Global Error Interceptor**: Friendly responses for 401, 500, or malformed input.

---

## 📁 Project Structure

```
backend/
├── controller/
├── service/
├── model/
├── exception/
├── util/

frontend/
├── components/
├── services/
├── App.js
├── App.css
```

---

## 📬 Contributions & License

Pull requests and improvements are welcome!  
MIT License – Free to use and modify.

---

✅ Built for developers and data engineers working with ClickHouse at scale.
---
