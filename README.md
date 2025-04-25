# ⚡ Bidirectional ClickHouse ↔ FlatFile Ingestion Tool

A full-stack ingestion platform built using **React + Vite** on the frontend and **Spring Boot** on the backend, supporting large-scale CSV-based ingestion and export for **ClickHouse**. It enables real-time streaming, progress tracking, intelligent column selection, preview capabilities, and secure authentication.

---

## 🚀 Features

- 🔁 **Bidirectional Ingestion**: Upload CSV files to ClickHouse or download CSV data from tables.
- 📄 **Preview Mode**: See first 100 rows of your CSV file before uploading.
- 📤 **Streamed Uploads**: Large CSV files streamed using `multipart/form-data` with chunked processing.
- 📥 **Streamed Downloads**: CSVs are streamed directly from ClickHouse with real-time progress.
- 📊 **Column Selection**: Choose columns to upload or download with per-column data types.
- 🔐 **Secure Auth**: Supports both JWT-based and password-based authentication.
- 🛠 **JOIN Support**: Download data with optional table joins and custom join conditions.
- 🔧 **Custom Delimiters**: Use `,`, `\t`, `|`, or any delimiter you choose.
- 📶 **Live Progress**: Upload/download progress is tracked in MB and percentage.
- 🧠 **Schema Aware**: Auto-populates column types and detects conflicts.
- ✅ **Robust Validation**: Zod-based schema validation for connection configs and file metadata.
- ⚠️ **Friendly Errors**: Graceful UI responses for ClickHouse errors and invalid inputs.

---

## ⚙️ Tech Stack

- **Frontend**: React (Vite) + Tailwind + TypeScript + Zod + Axios
- **Backend**: Java 17, Spring Boot 3, Streaming I/O, Univocity CSV Parser
- **Database**: ClickHouse 22.x or later
- **Other Tools**: MultipartFile handling

---

## 🖥️ Setup Instructions

### 🔁 Clone the Repository

```bash
git clone https://github.com/Kendo007/bidirectional.git
git clone https://github.com/Kendo007/bidirectional-frontend.git
```

---

### 🧱 Backend Setup (Spring Boot)

1. Open the `bidirectional` directory in your IDE.
2. Configure CORS policy in `src/main/resources/application.properties`:

   ```properties
   # application.properties
   config.frontend=http://localhost:5173
   ```

3. Run the Spring Boot backend:

```bash
cd bidirectional
mvn spring-boot:run
```

> ✅ Backend will start on `http://localhost:8081/`

---

### 🎨 Frontend Setup (Vite + React)

1. Navigate to frontend:

```bash
cd bidirectional-frontend
```

2. Create a `.env` file:

```env
VITE_SPRING_BOOT_URL=http://localhost:8081
```

> ⚠️ This should match your Spring Boot backend URL.

3. Install dependencies:

```bash
npm install
```

4. Start the development server:

```bash
npm run dev
```

> ✅ Frontend will be served at `http://localhost:5173`

---

## 📤 Upload to ClickHouse

- Select table (new or existing)
- Choose delimiter
- Preview first 100 rows
- Pick columns and types
- Upload with real-time progress feedback

---

## 📥 Download from ClickHouse

- Enter ClickHouse config
- Load tables and columns
- Select desired columns
- Choose delimiter
- Apply JOIN conditions (optional)
- Stream and download filtered CSV

---

## 📁 Project Structure

```
bidirectional/
├── src/main/java/org/example/
│   ├── controller/
│   ├── service/
│   ├── model/
│   ├── exception/
│   └── util/

bidirectional-frontend/
├── src/
│   ├── components/
│   ├── pages/
│   ├── lib/
│   ├── hooks/
│   └── styles/
├── .env
├── vite.config.ts
```

---