import pandas as pd
import numpy as np
import goslate
#from googletrans import Translator
#from translate import Translator
from deep_translator import GoogleTranslator
from tqdm import tqdm


# The translation function to loop through columns and rows
def translate_text(text):
    if pd.isna(text) or not isinstance(text, str):  # Skip NaN and non-string values
        return text
    try:
        return GoogleTranslator(source="th", target="en").translate(text)
    except Exception as e:
        print(f"Translation error: {e}")
        return text  # Return original text in case of error

# Wrap tqdm around the DataFrame to track progress
def translate_dataframe(df):
    tqdm.pandas()  # Initialize tqdm for pandas
    return df.progress_applymap(translate_text)  # Apply function across all cells with progress bar

# Main function
def main():
    # Read the CSV file into a DataFrame
    df = pd.read_csv("/Users/lb962/Documents/GitHub/MyanmarEQ2025/traffy_processing/traffy_data/bangkok_traffy.csv", delimiter=',')
    selected_df = df[["type", "comment"]]

    # Translate the DataFrame
    df_translated = translate_dataframe(selected_df)

    # Save the translated DataFrame as a CSV file
    df_translated.to_csv("/Users/lb962/Documents/GitHub/MyanmarEQ2025/traffy_processing/traffy_data/Translated.csv", index=False)
    print("Translation complete and saved as 'Translated.csv'.")

# Ensure the main function is called only when this script is executed directly
if __name__ == "__main__":
    main()
