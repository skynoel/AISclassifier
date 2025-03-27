import pathlib
from queue import Queue
from tkinter.filedialog import askdirectory
import ttkbootstrap as ttk
from ttkbootstrap.constants import *
from tkinter.scrolledtext import ScrolledText
from tkinter.filedialog import askdirectory
import os
import pandas as pd
import re

class FileSearchEngine(ttk.Frame):

    queue = Queue()
    searching = False

    def __init__(self, master):
        super().__init__(master, padding=15)
        self.pack(fill=BOTH, expand=YES)

        # application variables
        _path = pathlib.Path().absolute().as_posix()
        self.path_var = ttk.StringVar(value=_path)
        self.term_var = ttk.StringVar(value='md')
        self.type_var = ttk.StringVar(value='endswidth')
        self.path_new = str(_path)

        # header and labelframe option container
        option_text = "choose a directory to classify"
        self.option_lf = ttk.Labelframe(self, text=option_text, padding=15)
        self.option_lf.pack(fill=X, expand=YES, anchor=N)

        self.create_path_row()
        self.create_type_row()
        self.create_btn_row()


        output_container = ttk.Frame(self, padding=15)
        output_container.pack(fill=X, expand=YES,anchor=N)
        self.st = ScrolledText(output_container)
        self.st.pack(fill=BOTH, expand=YES)
        

    def create_path_row(self):
        """Add path row to labelframe"""
        path_row = ttk.Frame(self.option_lf)
        path_row.pack(fill=X, expand=YES)
        path_lbl = ttk.Label(path_row, text="Path", width=8)
        path_lbl.pack(side=LEFT, padx=(15, 0))
        path_ent = ttk.Entry(path_row, textvariable=self.path_var)
        path_ent.pack(side=LEFT, fill=X, expand=YES, padx=5)
        browse_btn = ttk.Button(
            master=path_row, 
            text="Browse", 
            command=self.on_browse, 
            width=8
        )
        browse_btn.pack(side=LEFT, padx=5)


    def create_type_row(self):
        """Add type row to labelframe"""
        type_row = ttk.Frame(self.option_lf)
        type_row.pack(fill=X, expand=YES)
        type_lbl = ttk.Label(type_row, text="Type", width=8)
        type_lbl.pack(side=LEFT, padx=(15, 0))

        contains_opt = ttk.Radiobutton(
            master=type_row, 
            text="none", 
            variable=self.type_var, 
            value="none"
        )
        contains_opt.pack(side=LEFT)

        startswith_opt = ttk.Radiobutton(
            master=type_row, 
            text="type1", 
            variable=self.type_var, 
            value="type1"
        )
        startswith_opt.pack(side=LEFT, padx=15)

        endswith_opt = ttk.Radiobutton(
            master=type_row, 
            text="type2", 
            variable=self.type_var, 
            value="type2"
        )
        endswith_opt.pack(side=LEFT)
        endswith_opt.invoke()

    def on_browse(self):
        """Callback for directory browse"""
        path = askdirectory(title="Browse directory")
        if path:
            self.path_var.set(path)
            self.path_new = path
    def create_btn_row(self):
        """Add term row to labelframe"""
        term_row = ttk.Frame(self.option_lf)
        term_row.pack(fill=X, expand=YES, pady=15)
        start_btn = ttk.Button(master=term_row,text="Start",command=self.start ,bootstyle=OUTLINE,width=8)
        start_btn.pack(side=LEFT, padx=5)
        close_button = ttk.Button(master=term_row, text="close",command=self.on_close,bootstyle="danger")
        close_button.pack(side=LEFT, padx=5)
    def on_close(self):
        self.quit()
        self.destroy()
    def start(self):
        folder_path = self.path_new

        if folder_path:
            output_folder = os.path.join(os.path.dirname(folder_path), f"classify_{os.path.basename(folder_path)}")
            os.makedirs(output_folder, exist_ok=True)
            self.st.insert(END, f"處理中: {folder_path}\n")
            self.st.update()

            for file_name in os.listdir(folder_path):
                if file_name.endswith(".csv") or file_name.endswith(".CSV"):
                    file_path = os.path.join(folder_path, file_name)
                    self.st.insert(END, f"開始處理: {file_name}\n")
                    self.st.update()
                    AIS_classifier(file_path, output_folder, self.st)  # 傳遞 self.st
                    self.st.insert(END, f"處理完成: {file_name}\n")
                    self.st.update()

def AIS_classifier(file_path, output_folder, output_widget):
    # 從檔名中提取日期（假設日期格式為 YYYYMMDD）
    file_name = os.path.basename(file_path)
    date_match = re.search(r"\d{8}", file_name)  # 匹配 8 位數字作為日期
    if not date_match:
        output_widget.insert(END, f"Error: No valid date found in file name '{file_name}'.\n")
        output_widget.see(END)
        output_widget.update()
        return
    date_value = date_match.group()  # 提取日期部分

    # 建立已存在的 MMSI 檔案對應表
    file_mapping = {
        re.match(r"^\d{9}", file_name).group(): file_name
        for file_name in os.listdir(output_folder)
        if re.match(r"^\d{9}", file_name)
    }

    # 讀取 CSV 檔案
    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        output_widget.insert(END, f"Error reading file '{file_name}': {e}\n")
        output_widget.see(END)
        output_widget.update()
        return

    # 檢查並兼容欄位名稱
    if "mmsi" not in df.columns and "SOURCE_ID" not in df.columns:
        output_widget.insert(END, "Error: Neither 'mmsi' nor 'SOURCE_ID' column found in the file.\n")
        output_widget.see(END)
        output_widget.update()
        return
    if "SOURCE_ID" in df.columns:
        df.rename(columns={"SOURCE_ID": "mmsi"}, inplace=True)  # 將 SOURCE_ID 重命名為 mmsi

    # 移除 mmsi 欄位中的空值
    df = df.dropna(subset=["mmsi"])

    # 依據 MMSI 分組並處理
    for mmsi_value, group in df.groupby("mmsi"):
        mmsi_value = str(mmsi_value)  # 確保 MMSI 是字串
        output_file = os.path.join(output_folder, f"{mmsi_value}({date_value}).csv")

        if str(mmsi_value) in file_mapping:
            # 如果檔案已存在，讀取並合併
            existing_file = os.path.join(output_folder, file_mapping[str(mmsi_value)])
            existing_df = pd.read_csv(existing_file)
            combined_df = pd.concat([existing_df, group], ignore_index=True)
        else:
            # 如果檔案不存在，直接使用當前分組
            combined_df = group

        combined_df.drop(combined_df.columns[0], axis=1, inplace=True)

        # 重設索引，並將索引值從 1 開始
        combined_df.reset_index(drop=True, inplace=True)
        combined_df.index += 1  # 將索引值從 1 開始
        combined_df.index.name = "Index"  # 設定索引欄位名稱為 "Index"

        

        # 將結果寫回 CSV
        combined_df.to_csv(output_file, index=True)  # 保留索引作為第一欄

        # 在介面上顯示處理進度
        output_widget.insert(END, f"Processed MMSI {mmsi_value}, saved to {output_file}\n")
        output_widget.see(END)
        output_widget.update()
    

if __name__ == '__main__':

    app = ttk.Window(title="AISclassifier", themename="solar",size=[1000,800])
    FileSearchEngine(app)
    app.iconbitmap('icon.ico')
    app.mainloop()
