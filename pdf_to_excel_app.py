import os
import threading
# import tkinter as tk
# from tkinter import filedialog, messagebox
# from tkinter import ttk
from datetime import datetime
import time
import pandas as pd

from lista_asistencia_process import extraer_datos_pdf_parallel, guardar_en_excel

# class PDFtoExcelApp(tk.Tk):
class PDFtoExcelApp():
    def __init__(self):
        super().__init__()
        
        self._pdf_path = None
        self._excel_path = None
        self._output_file = None
        self.num_workers = 2  # Valor predeterminado: "Adecuado"

    @property
    def pdf_path(self):
        return self._pdf_path
    @pdf_path.setter
    def pdf_path(self, value):
        self._pdf_path = value

    @property
    def excel_path(self):
        return self._excel_path
    @excel_path.setter
    def excel_path(self, value):
        self._excel_path = value

    @property
    def output_file(self):
        return self._output_file
    @output_file.setter
    def output_file(self, value):
        self._output_file = value

    def set_ui(self):
        pass
        # self.title("Lista de asistencia a Excel")
        # self.geometry("400x300")
        # self.label_title = tk.Label(self, text="Lista de asistencia a Excel", font=("Arial", 16))
        # self.label_title.pack(pady=10)

        # self.label_performance = tk.Label(self, text="Selecciona el nivel de performance:")
        # self.label_performance.pack(pady=5)
        # self.performance_selector = ttk.Combobox(self, values=["Regular", "Adecuado", "Elevado"], state="readonly")
        # self.performance_selector.current(1)  
        # self.performance_selector.pack(pady=5)
        # self.performance_selector.bind("<<ComboboxSelected>>", self.actualizar_workers)

        # botones_frame = tk.Frame(self)
        # botones_frame.pack(pady=5)

        # self.btn_cargar_pdf = tk.Button(botones_frame, text="Cargar PDF", command=self.cargar_pdf, width=15)
        # self.btn_cargar_pdf.pack(side=tk.LEFT, padx=5)

        # self.btn_cargar_excel = tk.Button(botones_frame, text="Cargar Excel", command=self.cargar_excel, width=15)
        # self.btn_cargar_excel.pack(side=tk.LEFT, padx=5)

        # self.label_pdf = tk.Label(self, text="No se ha seleccionado un archivo PDF", font=("Arial", 10))
        # self.label_pdf.pack(pady=2)

        # self.label_excel = tk.Label(self, text="No se ha seleccionado un archivo Excel", font=("Arial", 10))
        # self.label_excel.pack(pady=2)

        # self.btn_convertir = tk.Button(self, text="Ejecutar Conversión a Excel", command=self.ejecutar_conversion, state=tk.DISABLED)
        # self.btn_convertir.pack(pady=5)

        # self.progress = ttk.Progressbar(self, orient="horizontal", length=300, mode="determinate")
        # self.progress.pack(pady=10)

        # self.label_progreso = tk.Label(self, text="", font=("Arial", 10))
        # self.label_progreso.pack()        

    # def actualizar_workers(self, event):
    #     performance = self.performance_selector.get()
    #     if performance == "Regular":
    #         self.num_workers = 3
    #     elif performance == "Adecuado":
    #         self.num_workers = 5
    #     elif performance == "Elevado":
    #         self.num_workers = 10

    # def verificar_archivos_cargados(self):
    #     if self._pdf_path and self._excel_path:
    #         self.btn_convertir.config(state=tk.NORMAL)
    #     else:
    #         self.btn_convertir.config(state=tk.DISABLED)
            
    # def cargar_pdf(self):
    #     self._pdf_path = filedialog.askopenfilename(filetypes=[("Archivos PDF", "*.pdf")], initialdir=os.getcwd())
    #     if self._pdf_path:
    #         self.label_pdf.config(text=f"PDF: {os.path.basename(self._pdf_path)}")
    #         self.verificar_archivos_cargados()

    # def cargar_excel(self):
    #     self._excel_path = filedialog.askopenfilename(filetypes=[("Archivos Excel", "*.xlsx *.xls")], initialdir=os.getcwd())
    #     if self._excel_path:
    #         result = self.validar_carga_excel(self._excel_path)
    #         self.label_excel.config(text=result["label_excel"])
    #         self._excel_path = result["excel_path"]
    #         self.verificar_archivos_cargados()

    def validar_carga_excel(self, excel_path):
        try:
            df_excel = pd.read_excel(self._excel_path, dtype=str)  # Leer todo como string
            
            # Verificar que no falten las columnas requeridas
            columnas_requeridas = {'Codigo Empleado', 'Tipo Jornada'}
            if not columnas_requeridas.issubset(df_excel.columns):
                # self.label_excel.config(text="Faltan columnas requeridas: 'Codigo Empleado' y/o 'Tipo Jornada'.")
                excel_path = None
                # self.verificar_archivos_cargados()
                return {'label_excel': "Faltan columnas requeridas: 'Codigo Empleado' y/o 'Tipo Jornada'.", 'valid': False}
            
            # Verificar unicidad de 'Codigo Empleado'
            if not df_excel['Codigo Empleado'].is_unique:
                # self.label_excel.config(text="Los valores en 'Codigo Empleado' no son únicos.")
                excel_path = None
                # self.verificar_archivos_cargados()
                return {'label_excel': "Los valores en 'Codigo Empleado' no son únicos.", 'valid': False}
            
            # Si todo es correcto, mostrar el nombre del archivo
            # self.label_excel.config(text=f"Excel: {os.path.basename(self._excel_path)}")
            # self.verificar_archivos_cargados()
            return {'label_excel': f"Excel: {os.path.basename(excel_path)}", 'valid': True}
        except Exception as ex:
            return {'label_excel': ex.args[0] , 'valid': False}

    def actualizar_progreso(self, paginas_procesadas, total_paginas):
        pass
        # self.progress["maximum"] = total_paginas
        # self.progress["value"] = paginas_procesadas
        # self.label_progreso.config(text=f"Procesando página {paginas_procesadas}/{total_paginas}")
        # self.update_idletasks()

    def ejecutar_conversion(self):
        result = self.validar_carga_excel(self._excel_path)
        if not result['valid']:
            raise(ValueError(result['label_excel']))

        if not self._pdf_path or not self._excel_path:
            return
        
        # self.progress["value"] = 0
        # threading.Thread(target=self.procesar_pdf).start()
        target=self.procesar_pdf()

    def procesar_pdf(self):
        fecha_actual = datetime.now().strftime("%y-%m-%d")
        output_default = f"{os.path.splitext(os.path.basename(self._pdf_path))[0]} {fecha_actual}.xlsx"

        # self._output_file = filedialog.asksaveasfilename(defaultextension=".xlsx", initialfile=output_default, filetypes=[("Archivos Excel", "*.xlsx")])
        self._output_file = f"{output_default}-{time.time()}.xlsx"

        if self._output_file:
            data = extraer_datos_pdf_parallel(self._pdf_path, num_workers=self.num_workers, progress_callback=self.actualizar_progreso)
            # self.label_progreso.config(text="Creando archivo Excel, por favor espere...")
            guardar_en_excel(data, self._output_file, self._pdf_path, self._excel_path, num_workers=self.num_workers)
            # messagebox.showinfo("Proceso completado", "¡Proceso completado con éxito!")
            # self.label_pdf.config(text="No se ha seleccionado un archivo PDF")
            # self.label_excel.config(text="No se ha seleccionado un archivo Excel")
            # self.btn_convertir.config(state=tk.DISABLED)
            # self.progress["value"] = 0
            # self.label_progreso.config(text="")

