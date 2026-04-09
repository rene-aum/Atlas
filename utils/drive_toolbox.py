from datetime import datetime
import pandas as pd
import pytz
from gspread_dataframe import set_with_dataframe,get_as_dataframe
import io
import time
import requests
import json
import math



def from_drive_to_local(drive, id_file, file_name):
    """moves file from google drive to current local directory
       drive: GoogleDrive object
       id_file: id of the drive file
       file_name: name of the file to use in the local directory
    """
    links = drive.CreateFile({'id':id_file})
    links.GetContentFile(file_name)
    return

def get_last_modification_date_drive(drive,sheet_id):
    id_ = sheet_id
    link = drive.CreateFile({'id':id_})
    timestamp_utc=link.GetRevisions()[-1].get('modifiedDate')
    dt_utc = datetime.fromisoformat(timestamp_utc.replace('Z', '+00:00'))
    mexico_city_tz = pytz.timezone('America/Mexico_City')
    dt_mexico_city = dt_utc.astimezone(mexico_city_tz)
    formatted_timestamp = dt_mexico_city.strftime('%Y-%m-%d %H:%M:%S %Z%z')
    data_date = pd.to_datetime(formatted_timestamp).strftime('%Y-%m-%d')
    return data_date

def create_sheets_in_drive_folder(gc,file_name,folder_id,df_to_set=None):

    spreadsheet = gc.create(file_name, folder_id=folder_id)
    # spreadsheet.share('user@example.com', perm_type='user', role='writer') # Optional: share

    worksheet = spreadsheet.sheet1
    if df_to_set is not None:
        set_with_dataframe(worksheet, df_to_set)
    print(f"Google Sheet {file_name} created and updated in folder ID: {folder_id}")

def update_sheets_in_drive_folder(
        gc,
        spreadsheet_id,
        worksheet_name,
        df_to_update,
        retries: int = 3,
        initial_delay: float = 2.0,
        backoff_factor: float = 2.0,
        ):
    """
    Update a Google Sheets worksheet with a DataFrame, retrying on failure.

    Parameters
    ----------
    gc : gspread.Client
        Authenticated gspread client.
    spreadsheet_id : str
        ID of the Google Sheet.
    worksheet_name : str
        Name of the worksheet to update.
    df_to_update : pandas.DataFrame
        DataFrame whose contents will replace the worksheet.
    retries : int, default 3
        Number of attempts in total (initial try + retries-1).
    initial_delay : float, default 2.0
        Seconds to sleep before the first retry.
    backoff_factor : float, default 2.0
        Multiplier applied to the delay after each failed attempt.
    """

    attempt = 0
    delay = initial_delay
    last_exception = None

    while attempt < retries:
        attempt += 1
        try:
            # 1. Open the existing spreadsheet by ID
            spreadsheet = gc.open_by_key(spreadsheet_id)

            # 2. Access the worksheet by name
            worksheet = spreadsheet.worksheet(worksheet_name)

            # 3. Clear the existing content of the worksheet
            worksheet.clear()

            # 4. Update the worksheet with the DataFrame
            set_with_dataframe(worksheet, df_to_update)

            print(
                f"[attempt {attempt}/{retries}] "
                f"Google Sheet {spreadsheet_id!r} - {worksheet_name!r} "
                f"updated with new data."
            )
            return  # success → exit the function

        except Exception as e:
            last_exception = e
            print(
                f"[attempt {attempt}/{retries}] "
                f"Failed to update sheet {spreadsheet_id!r} - {worksheet_name!r}: {e}"
            )

            if attempt >= retries:
                # no more retries left: re-raise or handle as you prefer
                print("Exhausted all retries; giving up.")
                raise

            # wait before the next retry
            print(f"Retrying in {delay} seconds...")
            time.sleep(delay)
            delay *= backoff_factor

def read_from_google_sheets(gc,spreadsheet_id,sheetname=None):
    """
    """

    # Open the Google Sheet using the extracted ID
    spreadsheet = gc.open_by_key(spreadsheet_id)
    if sheetname is  None:
        worksheet = spreadsheet.sheet1  # Or select a specific worksheet
    else:
        worksheet = spreadsheet.worksheet(sheetname)
    # Read the data into a pandas DataFrame
    df = get_as_dataframe(worksheet,
                          evaluate_formulas=True,
                        value_render_option="UNFORMATTED_VALUE")

    return df

def list_file_ids_for_drive_folder(drive, folder_id:str):
    file_list = drive.ListFile({'q': f"'{folder_id}' in parents and trashed=false"}).GetList()
    file_id_dict = {}
    for file in file_list:
        file_id_dict[file['title']] = file['id']
    return file_id_dict

def read_csv_from_drive(drive,file_id):
    """
    """
    file = drive.CreateFile({'id': file_id})
    csv_bytes = file.GetContentString()  # returns CSV as a text string

    # --- Load into pandas ---
    df = pd.read_csv(io.StringIO(csv_bytes))
    return df

def write_csv_to_drive(drive,file_id, df):
    """Ya debe existir el archivo csv en drive y por tanto el file_id
        df: pandas dataframe
    """
    # Convert to CSV
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)

    # Load existing Drive file by ID
    file = drive.CreateFile({"id": file_id})
    file.SetContentString(csv_buffer.getvalue())
    file.Upload()    # <-- overwrites content, keeps same file ID

    print("Updated successfully.")

def create_csv_file_in_drive_folder(drive,folder_id,df,filename):
    """filename: string with extension .csv
    """
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    csv_str = csv_buffer.getvalue()
    file_metadata = {
                    "title": filename,   # what the user sees in Drive
                    "mimeType": "text/csv",
                    "parents": [{"id": folder_id}] 
                        }
    file = drive.CreateFile(file_metadata)
    file.SetContentString(csv_str)  # upload from string
    file.Upload()
    print("Uploaded file ID:", file["id"])
    return file["id"]

def send_google_chat_notification(webhook_url:str,msg:str):
    # TWebhook
    try:
        # Mensaje
        payload = {
            "text": f"*{msg}*"
        }

        # Realizar el envío
        response = requests.post(
            webhook_url,
            data=json.dumps(payload),
            headers={'Content-Type': 'application/json; charset=UTF-8'}
        )

        if response.status_code == 200:
            print("Notificación enviada a Google Chat.")
        else:
            print(f"Error al enviar: {response.status_code}")

    except Exception as e:
        print(f"Error en la función: {e}")

def update_sheets_in_drive_folder_chunked(
        gc,
        spreadsheet_id,
        worksheet_name,
        df_to_update,
        chunk_size: int = 10000,
        retries: int = 3,
        initial_delay: float = 2.0,
        backoff_factor: float = 2.0,
        clear_first: bool = True,
        include_header: bool = True,
        ):
    """
    Update a Google Sheets worksheet with a DataFrame in row chunks.

    Parameters
    ----------
    gc : gspread.Client
        Authenticated gspread client.
    spreadsheet_id : str
        ID of the Google Sheet.
    worksheet_name : str
        Name of the worksheet to update.
    df_to_update : pandas.DataFrame
        DataFrame to write.
    chunk_size : int, default 10000
        Number of dataframe rows per write request.
    retries : int, default 3
        Number of attempts per chunk.
    initial_delay : float, default 2.0
        Seconds before the first retry.
    backoff_factor : float, default 2.0
        Retry backoff multiplier.
    clear_first : bool, default True
        Whether to clear the sheet before writing.
    include_header : bool, default True
        Whether to write dataframe headers in the first chunk.

    Notes
    -----
    - Uses worksheet.update(range_name=..., values=...) chunk by chunk.
    - Keeps the sheet write pattern much lighter than one giant payload.
    """

    spreadsheet = gc.open_by_key(spreadsheet_id)
    worksheet = spreadsheet.worksheet(worksheet_name)

    if clear_first:
        worksheet.clear()

    n_rows, n_cols = df_to_update.shape

    if include_header:
        total_rows_to_write = n_rows + 1
    else:
        total_rows_to_write = n_rows

    if total_rows_to_write == 0:
        print(f"Sheet {spreadsheet_id!r} - {worksheet_name!r}: nothing to write.")
        return

    def _colnum_to_a1(col_num: int) -> str:
        result = ""
        while col_num > 0:
            col_num, rem = divmod(col_num - 1, 26)
            result = chr(65 + rem) + result
        return result

    last_col_letter = _colnum_to_a1(n_cols)

    # write header first
    start_row = 1
    if include_header:
        header_values = [df_to_update.columns.astype(str).tolist()]
        header_range = f"A1:{last_col_letter}1"
        worksheet.update(
            range_name=header_range,
            values=header_values,
            value_input_option="RAW"
        )
        start_row = 2

    n_chunks = math.ceil(n_rows / chunk_size)

    for chunk_idx in range(n_chunks):
        row_start = chunk_idx * chunk_size
        row_end = min((chunk_idx + 1) * chunk_size, n_rows)

        chunk_df = df_to_update.iloc[row_start:row_end]

        # Replace NaN with empty string for Sheets
        values = chunk_df.where(pd.notnull(chunk_df), "").values.tolist()

        sheet_row_start = start_row + row_start
        sheet_row_end = sheet_row_start + len(values) - 1
        range_name = f"A{sheet_row_start}:{last_col_letter}{sheet_row_end}"

        attempt = 0
        delay = initial_delay

        while attempt < retries:
            attempt += 1
            try:
                worksheet.update(
                    range_name=range_name,
                    values=values,
                    value_input_option="RAW"
                )
                print(
                    f"[chunk {chunk_idx + 1}/{n_chunks}] "
                    f"[attempt {attempt}/{retries}] "
                    f"Wrote rows {row_start}:{row_end} "
                    f"to {spreadsheet_id!r} - {worksheet_name!r}."
                )
                break

            except Exception as e:
                print(
                    f"[chunk {chunk_idx + 1}/{n_chunks}] "
                    f"[attempt {attempt}/{retries}] "
                    f"Failed writing rows {row_start}:{row_end} "
                    f"to {spreadsheet_id!r} - {worksheet_name!r}: {e}"
                )

                if attempt >= retries:
                    print("Exhausted retries for current chunk; giving up.")
                    raise

                print(f"Retrying chunk in {delay} seconds...")
                time.sleep(delay)
                delay *= backoff_factor

def insert_value_by_row_id_and_column_name(
        gc,
        spreadsheet_id: str,
        worksheet_name: str,
        row_id,
        column_name: str,
        value_to_insert,
        id_col: int = 1,
        header_row: int = 1,
        retries: int = 3,
        initial_delay: float = 2.0,
        backoff_factor: float = 2.0,
        ):
    """
    Insert/update a value in a column (by name) for the row whose ID matches `row_id`.

    Parameters
    ----------
    gc : gspread.Client
        Authenticated gspread client.
    spreadsheet_id : str
        ID of the Google Sheet.
    worksheet_name : str
        Name of the worksheet.
    row_id : Any
        Value to search in the ID column.
    column_name : str
        Header name of the column to update (must exist in header_row).
    value_to_insert : Any
        Value to write into the matched row and target column.
    id_col : int, default 1
        Column index (1-based) where the row ID is stored.
    header_row : int, default 1
        Row index where headers are located.
    retries : int, default 3
        Number of attempts in total.
    initial_delay : float, default 2.0
        Seconds before first retry.
    backoff_factor : float, default 2.0
        Retry backoff multiplier.

    Returns
    -------
    dict
        Metadata about the update.
    """

    attempt = 0
    delay = initial_delay

    while attempt < retries:
        attempt += 1
        try:
            # 1. Open spreadsheet
            spreadsheet = gc.open_by_key(spreadsheet_id)
            worksheet = spreadsheet.worksheet(worksheet_name)

            # 2. Read header row
            headers = worksheet.row_values(header_row)
            headers_normalized = [str(h).strip() for h in headers]

            if column_name not in headers_normalized:
                raise ValueError(
                    f"Column {column_name!r} not found in header row {header_row}."
                )

            target_col = headers_normalized.index(column_name) + 1  # 1-based index

            # 3. Read ID column
            id_values = worksheet.col_values(id_col)
            row_id_str = str(row_id).strip()

            matched_row = None
            for i, current_id in enumerate(id_values, start=1):
                if str(current_id).strip() == row_id_str:
                    matched_row = i
                    break

            if matched_row is None:
                raise ValueError(
                    f"Row ID {row_id!r} not found in column {id_col}."
                )

            # 4. Update cell
            worksheet.update_cell(matched_row, target_col, value_to_insert)

            print(
                f"[attempt {attempt}/{retries}] "
                f"Updated sheet {spreadsheet_id!r} - {worksheet_name!r}: "
                f"row_id={row_id!r}, matched_row={matched_row}, "
                f"column_name={column_name!r}, value={value_to_insert!r}"
            )

            return {
                "updated": True,
                "row_id": row_id,
                "matched_row": matched_row,
                "column_name": column_name,
                "value_inserted": value_to_insert,
            }

        except Exception as e:
            print(
                f"[attempt {attempt}/{retries}] "
                f"Failed to insert value into sheet {spreadsheet_id!r} - {worksheet_name!r}: {e}"
            )

            if attempt >= retries:
                print("Exhausted all retries; giving up.")
                raise

            print(f"Retrying in {delay} seconds...")
            time.sleep(delay)
            delay *= backoff_factor