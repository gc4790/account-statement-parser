import pandas as pd

with open(r"d:\BhumiDoc\inspect_output.txt", "w", encoding="utf-8") as f:
    def inspect(file_path):
        f.write(f"\n--- Inspecting: {file_path} ---\n")
        try:
            df = pd.read_excel(file_path)
            f.write(f"Columns: {df.columns.tolist()}\n")
            f.write("First 15 rows:\n")
            f.write(df.head(15).to_string() + "\n")
        except Exception as e:
            f.write(f"Error: {e}\n")

    inspect(r"d:\BhumiDoc\Flat Resident List-c wing.xlsx")
    inspect(r"d:\BhumiDoc\Flat Resident List.xlsx")
