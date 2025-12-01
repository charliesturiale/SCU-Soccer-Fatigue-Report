import report_catapult
import report_vald
import pandas as pd
from openpyxl import Workbook
from openpyxl.styles import Font, Alignment, PatternFill, Border, Side
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.styles import Color
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from models import Metric, Player, Roster, Team, PlayerMetricValue
from dotenv import load_dotenv

load_dotenv()

# No need to do much in here yet, because building profiles just updates sql
def generate_report_handler():

    catapult_report_df = report_catapult.get_catapult_report_metrics_main()

    forcedecks_report_df, nordbord_report_df = report_vald.get_vald_report_metrics_main()


    print("Report generation complete")
    print(f"Catapult data: {len(catapult_report_df)} players")
    print(f"ForceDecks data: {len(forcedecks_report_df)} players")
    print(f"NordBord data: {len(nordbord_report_df)} players")

    # FRONTEND / VISUALIZATION CODE GOES HERE (or calls to helper fns)
    create_report_table_and_export(catapult_report_df, forcedecks_report_df, nordbord_report_df)
    return

def create_report_table_and_export(catapult_report_df, forcedecks_report_df, nordbord_report_df):
    """
    Compile metrics from all three dataframes into a formatted Excel report.

    Players from Catapult serve as the master list. Metrics from ForceDecks and NordBord
    are merged in based on player_name. Missing data shows as N/A.
    """

    # ============================================================================
    # CONFIGURATION SECTION - Modify these settings as needed
    # ============================================================================

    # Define which metrics to include in the report (use metric codes from the dataframe)
    SELECTED_METRICS = {
        'catapult': [
            'total_distance',
            'high_speed_distance',
            'total_player_load',
            "gen2_acceleration_band6plus_total_effort_count"
        ],
        'forcedecks': [
            '6553607',  # Jump Height (Flight Time)
            '6553698',  # RSI-Modified
            '6553619'   # Concentric Mean Force
        ],
        'nordbord': [
            'leftMaxForce',
            'rightMaxForce',
            'leftAvgForce',
            'rightAvgForce'
        ]
    }

    # Configure comparison type for each metric:
    # - 'reference': Compare to historical average (reference_value in database)
    # - 'previous': Compare to previous week (previous_value in database)
    METRIC_COMPARISON_CONFIG = {
        # Catapult metrics - use reference (historical average)
        'total_distance': 'reference',
        'high_speed_distance': 'reference',
        'total_player_load': 'reference',
        'gen2_acceleration_band6plus_total_effort_count': 'reference',

        # ForceDecks metrics - use previous week for more recent comparison
        '6553607': 'previous',
        '6553698': 'previous',
        '6553619': 'previous',

        # NordBord metrics - use previous week
        'leftMaxForce': 'previous',
        'rightMaxForce': 'previous',
        'leftAvgForce': 'previous',
        'rightAvgForce': 'previous'
    }

    # ============================================================================
    # END CONFIGURATION
    # ============================================================================

    # Get metric names from database for proper column headers
    metric_code_to_name = get_metric_names_from_db()

    # Start with catapult data (master list of players)
    report_df = catapult_report_df[['player_name']].copy()

    # Track which metric codes are in which columns (for formatting later)
    column_to_metric_code = {}

    # Add selected Catapult metrics
    for metric_code in SELECTED_METRICS['catapult']:
        if metric_code in catapult_report_df.columns:
            # Use the friendly name from database if available
            column_name = metric_code_to_name.get(metric_code, metric_code)
            report_df[column_name] = catapult_report_df[metric_code].round(2)
            column_to_metric_code[column_name] = metric_code

    # Merge ForceDecks data
    if not forcedecks_report_df.empty:
        for metric_code in SELECTED_METRICS['forcedecks']:
            if metric_code in forcedecks_report_df.columns:
                column_name = metric_code_to_name.get(metric_code, metric_code)
                temp_df = forcedecks_report_df[['player_name', metric_code]].copy()
                temp_df.rename(columns={metric_code: column_name}, inplace=True)
                report_df = report_df.merge(
                    temp_df,
                    on='player_name',
                    how='left'
                )
                # Round to 2 decimal places
                if column_name in report_df.columns:
                    report_df[column_name] = report_df[column_name].round(2)
                    column_to_metric_code[column_name] = metric_code

    # Merge NordBord data
    if not nordbord_report_df.empty:
        for metric_code in SELECTED_METRICS['nordbord']:
            if metric_code in nordbord_report_df.columns:
                column_name = metric_code_to_name.get(metric_code, metric_code)
                temp_df = nordbord_report_df[['player_name', metric_code]].copy()
                temp_df.rename(columns={metric_code: column_name}, inplace=True)
                report_df = report_df.merge(
                    temp_df,
                    on='player_name',
                    how='left'
                )
                # Round to 2 decimal places
                if column_name in report_df.columns:
                    report_df[column_name] = report_df[column_name].round(2)
                    column_to_metric_code[column_name] = metric_code

    # Get reference values from database for conditional formatting
    player_reference_values = get_player_reference_values()

    # Create Excel workbook
    wb = Workbook()
    ws = wb.active
    ws.title = "Match Report"

    # Write headers
    headers = list(report_df.columns)
    ws.append(headers)

    # Style the header row
    header_fill = PatternFill(start_color="366092", end_color="366092", fill_type="solid")
    header_font = Font(bold=True, color="FFFFFF", size=11)

    for col_num, header in enumerate(headers, 1):
        cell = ws.cell(row=1, column=col_num)
        cell.fill = header_fill
        cell.font = header_font
        cell.alignment = Alignment(horizontal="center", vertical="center")

    # Write data rows
    for row_data in dataframe_to_rows(report_df, index=False, header=False):
        ws.append(row_data)

    # Apply styling to data cells
    thin_border = Border(
        left=Side(style='thin'),
        right=Side(style='thin'),
        top=Side(style='thin'),
        bottom=Side(style='thin')
    )

    for row in ws.iter_rows(min_row=1, max_row=ws.max_row, max_col=ws.max_column):
        for cell in row:
            cell.border = thin_border
            if cell.row > 1:  # Data rows (not header)
                cell.alignment = Alignment(horizontal="center", vertical="center")

                # Handle N/A display for missing values
                if cell.value is None or (isinstance(cell.value, float) and pd.isna(cell.value)):
                    cell.value = "N/A"

    # Auto-adjust column widths
    for column in ws.columns:
        max_length = 0
        column_letter = column[0].column_letter

        for cell in column:
            try:
                if cell.value:
                    max_length = max(max_length, len(str(cell.value)))
            except:
                pass

        adjusted_width = min(max_length + 2, 20)  # Cap at 20 for portrait mode
        ws.column_dimensions[column_letter].width = adjusted_width

    # Apply conditional formatting based on background profile comparison
    apply_conditional_formatting(
        ws,
        report_df,
        player_reference_values,
        column_to_metric_code,
        METRIC_COMPARISON_CONFIG
    )

    # Save the Excel file
    output_dir = "Project/match-reports/output"
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, "match_report.xlsx")

    wb.save(output_path)
    print(f"\n{'='*60}")
    print(f"Match report exported successfully!")
    print(f"  Location: {output_path}")
    print(f"  Players: {len(report_df)}")
    print(f"  Metrics: {len(report_df.columns) - 1}")  # Exclude player_name
    print(f"{'='*60}\n")

    return


def get_metric_names_from_db():
    """
    Retrieve metric names from the database to use as column headers.

    Returns
    -------
    dict
        Dictionary mapping metric codes to friendly names
        Example: {'total_distance': 'Total Distance', '6553607': 'Jump Height (Flight Time)'}
    """
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print("Warning: DATABASE_URL not set, using metric codes as column names")
        return {}

    try:
        engine = create_engine(db_url)
        with Session(engine) as session:
            # Get all metrics from database
            all_metrics = session.query(Metric).all()

            metric_map = {metric.code: metric.name for metric in all_metrics}

            print(f"Loaded {len(metric_map)} metric names from database")
            return metric_map

    except Exception as e:
        print(f"Error loading metric names from database: {e}")
        return {}


def get_player_reference_values():
    """
    Retrieve reference and previous values for all players from the database.

    Returns
    -------
    dict
        Nested dictionary: {player_name: {metric_code: {'reference': val, 'previous': val}}}
    """
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print("Warning: DATABASE_URL not set, cannot load reference values")
        return {}

    try:
        engine = create_engine(db_url)
        with Session(engine) as session:
            # Query all player metric values with player and metric info
            query = session.query(
                Player.first_name,
                Player.last_name,
                Metric.code,
                PlayerMetricValue.reference_value,
                PlayerMetricValue.previous_value
            ).join(
                PlayerMetricValue, Player.id == PlayerMetricValue.player_id
            ).join(
                Metric, Metric.id == PlayerMetricValue.metric_id
            ).all()

            # Build nested dictionary
            player_values = {}
            for first_name, last_name, metric_code, ref_val, prev_val in query:
                player_name = f"{first_name} {last_name}".strip()

                if player_name not in player_values:
                    player_values[player_name] = {}

                player_values[player_name][metric_code] = {
                    'reference': float(ref_val) if ref_val is not None else None,
                    'previous': float(prev_val) if prev_val is not None else None
                }

            print(f"Loaded reference values for {len(player_values)} players")
            return player_values

    except Exception as e:
        print(f"Error loading player reference values from database: {e}")
        return {}


def apply_conditional_formatting(worksheet, dataframe, player_reference_values,
                                 column_to_metric_code, comparison_config):
    """
    Apply conditional formatting based on comparison to reference values from database.

    Color coding uses a smooth gradient:
    - Blue spectrum: Above reference (darker blue = further above)
    - White: At reference (within ±5%)
    - Red spectrum: Below reference (darker red = further below)

    The gradient uses percentage difference capped at ±30% for visualization.

    Parameters
    ----------
    worksheet : openpyxl.worksheet.worksheet.Worksheet
        The Excel worksheet to format
    dataframe : pd.DataFrame
        The report dataframe with player metrics
    player_reference_values : dict
        Nested dict with reference/previous values per player and metric
    column_to_metric_code : dict
        Maps column names to metric codes
    comparison_config : dict
        Configuration for which comparison type to use per metric
    """
    if not player_reference_values:
        print("No reference values available for conditional formatting")
        return

    def interpolate_color(percent_diff):
        """
        Create a smooth gradient from red (low) → white (reference) → blue (high).

        Parameters
        ----------
        percent_diff : float
            Percentage difference from reference value

        Returns
        -------
        tuple : (PatternFill, Font)
            The fill color and font to apply
        """
        # Cap the percentage difference for color calculation
        capped_diff = max(-75, min(75, percent_diff))

        # Define color endpoints (RGB values)
        # Blue for positive (higher than reference): RGB(68, 114, 196) = #4472C4
        blue = (68, 114, 196)
        # White for neutral: RGB(255, 255, 255) = #FFFFFF
        white = (255, 255, 255)
        # Red for negative (lower than reference): RGB(192, 80, 77) = #C0504D
        red = (192, 80, 77)

        # Small threshold around 0 to keep white (±5%)
        if abs(capped_diff) <= 5:
            r, g, b = white
            font_color = "000000"  # Black text
        elif capped_diff > 5:
            # Positive: interpolate from white to blue
            # Normalize to 0-1 range (5% to 75% maps to 0 to 1)
            ratio = min((capped_diff - 5) / 70, 1.0)
            r = int(white[0] + (blue[0] - white[0]) * ratio)
            g = int(white[1] + (blue[1] - white[1]) * ratio)
            b = int(white[2] + (blue[2] - white[2]) * ratio)
            # Use white text on darker blues
            font_color = "FFFFFF" if ratio > 0.5 else "000000"
        else:  # capped_diff < -5
            # Negative: interpolate from white to red
            # Normalize to 0-1 range (-5% to -75% maps to 0 to 1)
            ratio = min((abs(capped_diff) - 5) / 70, 1.0)
            r = int(white[0] + (red[0] - white[0]) * ratio)
            g = int(white[1] + (red[1] - white[1]) * ratio)
            b = int(white[2] + (red[2] - white[2]) * ratio)
            # Use white text on darker reds
            font_color = "FFFFFF" if ratio > 0.5 else "000000"

        # Convert RGB to hex
        hex_color = f"{r:02X}{g:02X}{b:02X}"

        fill = PatternFill(start_color=hex_color, end_color=hex_color, fill_type="solid")
        font = Font(color=font_color)

        return fill, font

    # Get column indices for each metric
    headers = [cell.value for cell in worksheet[1]]

    for col_idx, column_name in enumerate(headers, 1):
        # Skip player_name column
        if column_name == 'player_name':
            continue

        # Get the metric code for this column
        metric_code = column_to_metric_code.get(column_name)
        if not metric_code:
            continue

        # Get comparison type for this metric
        comparison_type = comparison_config.get(metric_code, 'reference')

        # Iterate through each player row
        for row_idx in range(2, worksheet.max_row + 1):
            player_name_cell = worksheet.cell(row=row_idx, column=1)
            player_name = player_name_cell.value

            value_cell = worksheet.cell(row=row_idx, column=col_idx)
            current_value = value_cell.value

            # Skip if no data or N/A
            if current_value is None or current_value == "N/A" or not isinstance(current_value, (int, float)):
                continue

            # Get reference value for this player and metric
            if player_name not in player_reference_values:
                continue

            player_metrics = player_reference_values[player_name]
            if metric_code not in player_metrics:
                continue

            # Choose which reference value to use based on config
            if comparison_type == 'previous':
                reference_value = player_metrics[metric_code]['previous']
            else:  # 'reference' or default
                reference_value = player_metrics[metric_code]['reference']

            if reference_value is None or reference_value == 0:
                continue

            # Calculate percentage difference
            percent_diff = ((current_value - reference_value) / reference_value) * 100

            # Apply gradient color based on percent difference
            fill, font = interpolate_color(percent_diff)
            value_cell.fill = fill
            value_cell.font = font

    print(f"Applied conditional formatting to {len(column_to_metric_code)} metric columns")


# RUN FILE
generate_report_handler()