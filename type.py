import os
import pandas as pd
from datetime import datetime
import chardet
import pathlib
from queue import Queue
import ttkbootstrap as ttk
from ttkbootstrap.constants import *
from tkinter.scrolledtext import ScrolledText
from tkinter.filedialog import askdirectory
import os
import re
import threading

def detect_encoding(file_path):
    with open(file_path, 'rb') as f:
        result = chardet.detect(f.read(10000))
        return result['encoding']

def mach(file_path, output_folder, output_widget):
    try:
        encoding = detect_encoding(file_path)
        df = pd.read_csv(file_path, encoding=encoding)
        output_widget.insert(END, f"正在處理檔案: {file_path} \n")
        output_widget.see(END)
        output_widget.update()

        # 欄位名稱兼容處理
        column_mapping = {
            'msg_type': 'msg_type',
            'MESSAGE_ID': 'msg_type',
            'GPS_year': 'GPS_year',
            'RMC_DATE_YEAR': 'GPS_year',
            'GPS_month': 'GPS_month',
            'RMC_DATE_MON': 'GPS_month',
            'GPS_day': 'GPS_day',
            'RMC_DATE_DAY': 'GPS_day',
            'ship_type': 'ship_type',
            'TYPE_OF_SHIP_AND_CARGO_TYPE': 'ship_type',
            'mmsi': 'mmsi',
            'SOURCE_ID': 'mmsi'
        }

        # 重命名欄位
        df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns}, inplace=True)

        if 'msg_type' not in df.columns:
            output_widget.insert(END, f"警告：{file_path} 中不存在 msg_type 欄位，跳過該檔案 \n")
            output_widget.see(END)
            output_widget.update()
            return

        df = df[df['msg_type'] == 5]  # 只保留 msg_type 欄位值為 5 的列
        if df.empty:
            output_widget.insert(END, f"警告：{file_path} 中不存在 msg_type 欄位值為 5 的資料，跳過該檔案 \n")
            output_widget.see(END)
            output_widget.update()
            return

        columns_to_keep = [
            'GPS_year', 'GPS_month', 'GPS_day', 'PACKET_TYPE', 'mmsi',
            'msg_type', 'ship_type'
        ]
        missing_columns = [col for col in columns_to_keep if col not in df.columns]
        if missing_columns:
            output_widget.insert(END, f"警告：{file_path} 缺少欄位: {missing_columns}，跳過該檔案 \n")
            output_widget.see(END)
            output_widget.update()
            return

        df = df[columns_to_keep]
        df = df.drop_duplicates(subset=['mmsi'], keep='first')

        if len(df) < 1:
            output_widget.insert(END, f"警告：{file_path} 資料不足，無法生成日期資訊 \n")
            output_widget.see(END)
            output_widget.update()
            return

        # 避免索引越界
        date_row = df.iloc[0]  # 使用第一筆資料
        date_str = f"{date_row['GPS_year']} {date_row['GPS_month']} {date_row['GPS_day']}"
        try:
            date = datetime.strptime(date_str, '%Y %m %d')
        except ValueError:
            output_widget.insert(END, f"日期格式錯誤：{date_str}，跳過該檔案 \n")
            output_widget.see(END)
            output_widget.update()
            return

        new_filename = f"shipDailyList_{date.strftime('%Y%m%d')}.csv"
        new_file_path = os.path.join(output_folder, new_filename)
        df.to_csv(new_file_path, index=False)
        output_widget.insert(END, f"{new_filename} 已儲存 \n")
        output_widget.see(END)
        output_widget.update()
    except Exception as e:
        output_widget.insert(END, f"讀取 {file_path} 時發生錯誤: {e} \n")
        output_widget.see(END)
        output_widget.update()
        return

def count_ship(output_widget,input_directory, output_directory):
    csv_files = sorted([f for f in os.listdir(input_directory) if f.endswith('.csv')])
    first_file = csv_files[0]
    df_first = pd.read_csv(os.path.join(input_directory, first_file), encoding='ISO-8859-1')
    month = str(df_first['GPS_month'].iloc[0]).zfill(2)
    ship_type_counts = {}
    # Process each file
    for csv_file in csv_files:                
        file_path = os.path.join(input_directory, csv_file)
        df = pd.read_csv(file_path, encoding='ISO-8859-1')  # Specify the encoding
        output_widget.insert(END,f"處理中: {file_path} \n")
        output_widget.see(END)
        output_widget.update()
        # Create the date in YYYYMMDD format
        date = str(df['GPS_year'].iloc[0]) + \
               str(df['GPS_month'].iloc[0]).zfill(2) + \
               str(df['GPS_day'].iloc[0]).zfill(2)

        # Count the occurrences of each ship type
        ship_type_counts[date] = df['ship_type'].value_counts()

    # Create a DataFrame with numbers 0-99 as shiptype
    shiptypes = list(range(100))
    summary_df = pd.DataFrame(shiptypes, columns=['shiptype'])

    # Fill the DataFrame with counts from each date
    for date, counts in ship_type_counts.items():
        summary_df[date] = summary_df['shiptype'].map(counts).fillna(0).astype(int)

    # Create an Excel writer object with the month included in the filename
    output_filename = f'ShipDailyList_{month}_統計總表.xlsx'
    output_path = os.path.join(output_directory, output_filename)
    writer = pd.ExcelWriter(output_path, engine='xlsxwriter')

    # Write the DataFrame to the Excel file
    summary_df.to_excel(writer, index=False, startrow=0, startcol=0)

    # Close the writer and save the file
    writer.close()

    print(f"Processed file saved to: {output_path}")



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
            text="mach", 
            variable=self.type_var, 
            value="mach"
        )
        contains_opt.pack(side=LEFT)

        startswith_opt = ttk.Radiobutton(
            master=type_row, 
            text="count", 
            variable=self.type_var, 
            value="count"
        )
        startswith_opt.pack(side=LEFT, padx=15)

        endswith_opt = ttk.Radiobutton(
            master=type_row, 
            text="filter", 
            variable=self.type_var, 
            value="filter"
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
        start_btn = ttk.Button(master=term_row,text="Start",command=self.start ,bootstyle="success-outline",width=8)
        start_btn.pack(side=LEFT, padx=5)
        close_button = ttk.Button(master=term_row, text="close",command=self.on_close,bootstyle="secondary-outline")
        close_button.pack(side=LEFT, padx=5)
        delete1_button = ttk.Button(master=term_row, text="deleteall",command=self.delete1,bootstyle="danger-outline")
        delete1_button.pack(side=LEFT, padx=5)
        delete2_button = ttk.Button(master=term_row, text="deletedaliy",command=self.delete2,bootstyle="worning-outline")
        delete2_button.pack(side=LEFT, padx=5)
    def on_close(self):
        self.quit()
        self.destroy()
    def start(self):
        folder_path = self.path_new

        if folder_path:
            

            self.st.insert(END, f"處理中: {folder_path}\n")
            self.st.update()

            # 根據 type_var 的值選擇執行的程序
            selected_type = self.type_var.get()
            if selected_type == "mach":
                # 啟動新線程來執行目前的程序
                thread = threading.Thread(target=self.process_files, args=(folder_path,))
                thread.start()
            elif selected_type == "count":
                # 啟動新線程來執行 type1 的程序
                thread = threading.Thread(target=self.process_type1_files, args=(folder_path,))
                thread.start()
            elif selected_type == "filter":
                # 啟動新線程來執行 type2 的程序
                thread = threading.Thread(target=self.process_type2_files, args=(folder_path,))
                thread.start()
            else:
                self.st.insert(END, f"未知的類型選擇: {selected_type}\n")
                self.st.update()
                return

    def process_files(self, folder_path):
        output_folder = os.path.join(os.path.dirname(folder_path), f"result_{os.path.basename(folder_path)}")
        os.makedirs(output_folder, exist_ok=True)
        for file_name in os.listdir(folder_path):
            if file_name.endswith(".csv") or file_name.endswith(".CSV"):
                file_path = os.path.join(folder_path, file_name)
                self.st.insert(END, f"開始處理: {file_name}\n")
                self.st.update()
                mach(file_path, output_folder, self.st)  # 傳遞 self.st
                self.st.insert(END, f"處理完成: {file_name}\n")
                self.st.update()
            elif file_name.endswith(".xlsx") or file_name.endswith(".XLSX"):
                file_path = os.path.join(folder_path, file_name)
                # Convert .xlsx to .csv
                try:
                    xlsx_data = pd.read_excel(file_path)
                    csv_file_path = file_path.replace(".xlsx", ".csv").replace(".XLSX", ".csv")
                    xlsx_data.to_csv(csv_file_path, index=False)
                    self.st.insert(END, f"開始處理: {file_name}\n")
                    self.st.update()
                    # Process the converted CSV file
                    mach(csv_file_path, output_folder, self.st)
                    self.st.insert(END, f"處理完成: {file_name}\n")
                    os.remove(csv_file_path)  # 刪除轉換後的 CSV 檔案
                except Exception as e:
                    self.st.insert(END, f"Error converting {file_name} to CSV: {e}\n")
                    self.st.update()
            else:
                self.st.insert(END, f"無法處理的檔案: {file_name}\n")
                self.st.update()
        self.st.insert(END, f"處理完成: {folder_path}\n")
        self.st.update()
    def process_type1_files(self, folder_path):
        output_folder = os.path.join(os.path.dirname(folder_path), f"count_result_{os.path.basename(folder_path)}")
        os.makedirs(output_folder, exist_ok=True)        
        self.st.insert(END, f"開始處理 (type1): {folder_path}\n")
        self.st.update()
        count_ship(self.st,folder_path, output_folder)        
        self.st.insert(END, f"處理完成 (type1): {folder_path}\n")
        self.st.update()

    def process_type2_files(self, folder_path):
        self.st.insert(END, f"還沒開發\n")
        self.st.update()
    def delete2(self):
        folder_path = self.path_new
        output_folder = os.path.join(os.path.dirname(folder_path), f"classify_{os.path.basename(folder_path)}")
        if folder_path:
            for file_name in os.listdir(output_folder):
                if "merge" not in file_name:
                    file_path = os.path.join(output_folder, file_name)
                    if os.path.isfile(file_path):
                        os.remove(file_path)
            self.st.insert(END, f"每日刪除完成: {output_folder}\n")
            self.st.update()
    def delete1(self):
        folder_path = self.path_new 
        output_folder = os.path.join(os.path.dirname(folder_path), f"classify_{os.path.basename(folder_path)}")
        if folder_path:
            for file_name in os.listdir(output_folder):
                file_path = os.path.join(output_folder, file_name)
                if os.path.isfile(file_path):
                    os.remove(file_path)
            self.st.insert(END, f"全部刪除完成: {output_folder}\n")
            self.st.update()

if __name__ == '__main__':

    app = ttk.Window(title="MMSI_Sorting", themename="morph",size=[1000,800])
    FileSearchEngine(app)
    app.mainloop()
