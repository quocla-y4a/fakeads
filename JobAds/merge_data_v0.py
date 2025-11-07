import os
import glob
import pandas as pd
from datetime import datetime


def merge_job_data():
    try:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        date_str = datetime.now().strftime("%Y-%m-%d") #  ="2025-11-07"

        # --- Thư mục nguồn: chứa các file data của ngày hôm nay ---
        source_dir = os.path.join(base_dir, "data", date_str)
        if not os.path.exists(source_dir):
            print(f"⚠️ Không tìm thấy thư mục dữ liệu: {source_dir}")
            return

        # --- Tìm tất cả file CSV cần gộp ---
        csv_files = glob.glob(os.path.join(source_dir, "job_data_page_*.csv"))
        if not csv_files:
            print(f"⚠️ Không tìm thấy file CSV nào trong {source_dir}")
            return

        print(f"🔍 Tìm thấy {len(csv_files)} file để gộp:")
        for f in csv_files:
            print(f"   - {os.path.basename(f)}")

        # --- Gộp toàn bộ file ---
        dfs = []
        for file in csv_files:
            df = pd.read_csv(file)
            dfs.append(df)

        merged_df = pd.concat(dfs, ignore_index=True)

        # --- Loại bỏ trùng lặp (nếu có) ---
        merged_df.drop_duplicates(inplace=True)

        # --- Tạo thư mục lưu kết quả ---
        output_dir = os.path.join(base_dir, "merged_data", date_str)
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, "job_data.csv")

        # --- Lưu file ---
        merged_df.to_csv(output_path, index=False, encoding="utf-8-sig")
        print(f"✅ Dữ liệu đã được gộp và lưu tại: {output_path}")
        print(f"📊 Tổng số dòng sau khi gộp: {len(merged_df)}")

    except Exception as e:
        print(f"🚨 Lỗi khi gộp dữ liệu: {e}")


if __name__ == "__main__":
    merge_job_data()
