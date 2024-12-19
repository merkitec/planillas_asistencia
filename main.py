from io import BytesIO
import logging
from logging.handlers import TimedRotatingFileHandler
import os
import time
from typing import List
import uuid
from fastapi import FastAPI, File, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, FileResponse
from pdf_to_excel_app import PDFtoExcelApp
from pydantic import BaseModel
import uvicorn

# Configure logging
os.makedirs("logs", exist_ok=True)
logging.basicConfig(level=logging.DEBUG, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[TimedRotatingFileHandler('logs/log.log', when="D", backupCount=10,)],
                    datefmt='%Y-%m-%dT%H:%M:%S')
logger = logging.getLogger(__name__)
logging.getLogger("pdfminer.psparser").setLevel(level=logging.WARNING)
logging.getLogger("pdfminer.pdfinterp").setLevel(level=logging.WARNING)
logging.getLogger("pdfminer.pdfpage").setLevel(level=logging.WARNING)
logging.getLogger("pdfminer.pdfdocument").setLevel(level=logging.WARNING)

app = FastAPI(
    title="Backend API for Planillas-Asistencia",
    openapi_url="/api/v1/openapi.json"
)

origins = [
    "*"
    # "http://127.0.0.1:8001"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    # allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"]
)

@app.get("/")
async def root():
    return {"message": "Backend API for LLM-Base running..."}


# Upload files
os.makedirs("./uploads", exist_ok=True)

def save_file(file):
    contents = file.file.read()
    file_path = f"uploads/{uuid.uuid4()}__{file.filename}"
    with open(file_path, 'wb') as f:
        f.write(contents)        
    return file_path

@app.post("/upload_pdf_excel")
async def upload_pdf_excel(files: List[UploadFile] = File(...)):
    try:
        app = PDFtoExcelApp()

        if len(files) != 2:
            return {"status": 1, "message": f"Only 2 files can be processed at a time"}

        if not files[0].content_type.startswith("application/pdf"):
            logging.warning(f"Invalid type of the first file. Please upload a PDF file. {files[0].filename}")
            return { "status": 1, "message": "Invalid type of the first file. Please upload a PDF file."}        
        app.pdf_path = save_file(files[0])        

        if not files[1].content_type.startswith("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"):
            logging.warning(f"Invalid type of the second file. Please upload a Excel file. {files[1].filename}")
            return { "status": 1, "message": "Invalid type of the second file. Please upload a Excel file."}        
        app.excel_path = save_file(files[1])        

        app.ejecutar_conversion()
        return FileResponse(
            path=app.output_file, 
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", 
            filename=app.output_file
        )

    except Exception as ex:
        return {"status": 1, "message": f"There was an error uploading the file: {ex.args[0]}"}
    finally:
        files[0].file.close()
        files[1].file.close()


# @app.post("/upload_pdf")
# async def upload_pdf(file: UploadFile = File(...)):
#     try:
#         # Validate file type (optional)
#         if not file.content_type.startswith("application/pdf"):
#             logging.warning(f"Invalid file type. Please upload a PDF file. {file.filename}")
#             return { "status": 1, "message": "Invalid file type. Please upload a PDF file."}        
        
#         contents = file.file.read()
#         file_path = f"uploads/{uuid.uuid4()}__{file.filename}"
#         with open(file_path, 'wb') as f:
#             f.write(contents)        
#         # file.file.close()
#     except Exception as ex:
#         return {"status": 1, "message": f"There was an error uploading the file: {ex.args[0]}"}
#     finally:
#         file.file.close()

#     try:
#         app = PDFtoExcelApp()
#         # pdf_processor = PDFIngestAppService()
#         app.pdf_path = file_path
#         # document = pdf_processor.process_document(file_path)
#         # return {"status": 1, "message": f"Successfully uploaded & processed {file.filename}", 
#         #         "data" : DocumentInfo.to_json(document)}
#     except Exception as ex:
#         return {"status": 1, "message": f"There was an error uploading & processing the file: {ex}"}
 
# @app.post("/upload_excel")
# async def upload_excel(file: UploadFile = File(...)):
#     try:
#         # Validate file type (optional)
#         if not file.content_type.startswith("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"):
#             logging.warning(f"Invalid file type. Please upload a Excel file. {file.filename}")
#             return { "status": 1, "message": "Invalid file type. Please upload a Excel file."}        
        
#         contents = file.file.read()
#         file_path = f"uploads/{uuid.uuid4()}__{file.filename}"
#         with open(file_path, 'wb') as f:
#             f.write(contents)        
#         # file.file.close()
#     except Exception as ex:
#         return {"status": 1, "message": f"There was an error uploading the file: {ex.args[0]}"}
#     finally:
#         file.file.close()

#     try:
#         app = PDFtoExcelApp()
#         # pdf_processor = PDFIngestAppService()
#         app.excel_path_path = file_path
#         # document = pdf_processor.process_document(file_path)
#         # return {"status": 1, "message": f"Successfully uploaded & processed {file.filename}", 
#         #         "data" : DocumentInfo.to_json(document)}
#     except Exception as ex:
#         return {"status": 1, "message": f"There was an error uploading & processing the file: {ex}"}
        
    
# @app.post("/execute_convert/")
# async def download_execute_query(sql: str):
#     app = PDFtoExcelApp()
#     # data_store = DataEntityAppService()
#     app.ejecutar_conversion()
#     try:
#         # with open(app.output_file, "+rb") as file:
#         #     response = file.read()
#         # streamIO = BytesIO(response.encode('utf-8'))
#         # headers = {'Content-Disposition': f'attachment; filename="exec_qry_{time.time()}.json"'}
#         # return StreamingResponse(streamIO, media_type="application/json", headers=headers)
#         return FileResponse(
#             path=app.output_file, 
#             media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", 
#             filename=app.output_file
#         )
#     except Exception as ex:
#         return { "status": 1, "message": ex.args[0]}
    
if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)    