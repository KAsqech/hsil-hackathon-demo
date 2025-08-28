from pathlib import Path
import pandas as pd

DATA_FOLDER = Path.cwd()

for file in DATA_FOLDER.glob("*_Dataset_New_Taxonomy.csv"):
    try:
        df = pd.read_csv(file)
        print(f"\n📄 {file.name} Columns:")
        print(list(df.columns))
    except Exception as e:
        print(f"❌ Could not read {file.name}: {e}")
