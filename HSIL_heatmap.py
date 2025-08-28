import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from pathlib import Path

# Constants
DATA_FOLDER = Path.cwd()
REQUIRED_COLUMNS = {"component", "sub-component", "variable", "measure", "country", "iso code", "year", "value"}

def load_all_data():
    combined_df = []
    print("📥 Loading datasets...")
    
    for file in DATA_FOLDER.glob("*_Dataset_New_Taxonomy.csv"):
        try:
            df = pd.read_csv(file)
            df.columns = [col.strip().lower() for col in df.columns]

            # Normalize possible variant of 'value' column
            if 'val' in df.columns and 'value' not in df.columns:
                df.rename(columns={'val': 'value'}, inplace=True)

            if REQUIRED_COLUMNS.issubset(set(df.columns)):
                combined_df.append(df)
                print(f"✅ Loaded: {file.name}")
            else:
                missing = REQUIRED_COLUMNS - set(df.columns)
                print(f"⚠️ Skipped {file.name}: missing columns {missing}")
        except Exception as e:
            print(f"❌ Error loading {file.name}: {e}")
    
    if combined_df:
        return pd.concat(combined_df, ignore_index=True)
    else:
        print("❌ No valid data loaded. Exiting.")
        return pd.DataFrame()

def plot_bivariate_heatmap(df, var_x, var_y):
    df_filtered = df[df["variable"].isin([var_x, var_y])].copy()
    
    # Check numeric conversion
    df_filtered["value"] = pd.to_numeric(df_filtered["value"], errors="coerce")

    # Report non-null count
    non_null_counts = df_filtered.dropna(subset=["value"]).groupby("variable")["value"].count()
    print("\n📊 Non-null Value Count per Variable:")
    print(non_null_counts)

    # Pivot
    df_pivot = df_filtered.pivot_table(
        index=["country", "year"],
        columns="variable",
        values="value",
        aggfunc="mean"
    ).reset_index()

    if var_x not in df_pivot.columns or var_y not in df_pivot.columns:
        print("\n❌ One or both selected variables are not present in the data after pivoting.")
        return

    df_bivar = df_pivot[["country", "year", var_x, var_y]].dropna()
    
    # Filter to most common countries
    top_countries = df_bivar["country"].value_counts().nlargest(15).index
    df_bivar = df_bivar[df_bivar["country"].isin(top_countries)]

    # Create pivot for heatmap
    heatmap_data = df_bivar.pivot(index="country", columns="year", values=var_y)

    # Plot
    plt.figure(figsize=(12, 8))
    sns.heatmap(
        heatmap_data,
        cmap="RdYlGn_r",
        linewidths=0.2,
        annot=False,
        cbar_kws={"label": var_y}
    )
    plt.title(f"Bivariate Heatmap for '{var_y}' by Year and Country")
    plt.xlabel("Year")
    plt.ylabel("Country")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

# Load data
df_all = load_all_data()

# Run if valid data loaded
if not df_all.empty:
    available_vars = sorted(df_all["variable"].dropna().unique())
    print("\n📌 Available Variables:")
    for i, var in enumerate(available_vars, 1):
        print(f"{i}. {var}")

    var_x = input("\nEnter Variable X: ").strip()
    var_y = input("Enter Variable Y: ").strip()

    plot_bivariate_heatmap(df_all, var_x, var_y)
