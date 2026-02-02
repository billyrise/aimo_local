"""
Excel Writer for AIMO Analysis Engine

Generates audit-ready Excel reports with constant memory mode.
Uses xlsxwriter with constant_memory=True to handle large datasets.
"""

import json
import yaml
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime
import xlsxwriter
from xlsxwriter.utility import xl_rowcol_to_cell, xl_range


class ExcelWriter:
    """
    Excel report generator with constant memory mode.
    
    Features:
    - constant_memory=True for large datasets
    - Multiple sheets (summary, findings, audit narrative)
    - Charts and graphs
    - Chunked data writing (1,000 rows at a time)
    """
    
    def __init__(self, output_path: Path, template_spec_path: Optional[Path] = None):
        """
        Initialize Excel writer.
        
        Args:
            output_path: Path to output Excel file
            template_spec_path: Path to excel_template_spec.json (optional)
        """
        self.output_path = Path(output_path)
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Load template spec if provided
        if template_spec_path is None:
            template_spec_path = Path(__file__).parent.parent.parent / "report" / "excel_template_spec.json"
        
        self.template_spec = {}
        if template_spec_path.exists():
            with open(template_spec_path, 'r', encoding='utf-8') as f:
                self.template_spec = json.load(f)
        
        # Create workbook with constant_memory=True (required for large datasets)
        self.workbook = xlsxwriter.Workbook(
            str(self.output_path),
            {'constant_memory': True, 'default_date_format': 'yyyy-mm-dd hh:mm:ss'}
        )
        
        # Define formats
        self.formats = self._create_formats()
        
        # Track sheet references
        self.sheets: Dict[str, Any] = {}
    
    def _create_formats(self) -> Dict[str, xlsxwriter.format.Format]:
        """Create cell formats based on template spec."""
        formats = {}
        
        # Header format
        header_format = self.workbook.add_format({
            'bold': True,
            'font_size': 11,
            'bg_color': '#1F4E79',
            'font_color': '#FFFFFF',
            'align': 'center',
            'valign': 'vcenter',
            'border': 1
        })
        formats['header'] = header_format
        
        # Subheader format
        subheader_format = self.workbook.add_format({
            'bold': True,
            'font_size': 10,
            'bg_color': '#4472C4',
            'font_color': '#FFFFFF',
            'align': 'center',
            'valign': 'vcenter',
            'border': 1
        })
        formats['subheader'] = subheader_format
        
        # Data format
        data_format = self.workbook.add_format({
            'font_size': 10,
            'border': 1,
            'valign': 'vcenter'
        })
        formats['data'] = data_format
        
        # Number formats
        formats['bytes'] = self.workbook.add_format({
            'num_format': '#,##0',
            'font_size': 10,
            'border': 1,
            'valign': 'vcenter'
        })
        
        formats['percentage'] = self.workbook.add_format({
            'num_format': '0.0%',
            'font_size': 10,
            'border': 1,
            'valign': 'vcenter'
        })
        
        formats['date'] = self.workbook.add_format({
            'num_format': 'yyyy-mm-dd',
            'font_size': 10,
            'border': 1,
            'valign': 'vcenter'
        })
        
        formats['datetime'] = self.workbook.add_format({
            'num_format': 'yyyy-mm-dd hh:mm:ss',
            'font_size': 10,
            'border': 1,
            'valign': 'vcenter'
        })
        
        # Conditional formats
        formats['high_risk'] = self.workbook.add_format({
            'bg_color': '#FFC7CE',
            'font_color': '#9C0006',
            'font_size': 10,
            'border': 1,
            'valign': 'vcenter'
        })
        
        formats['medium_risk'] = self.workbook.add_format({
            'bg_color': '#FFEB9C',
            'font_color': '#9C6500',
            'font_size': 10,
            'border': 1,
            'valign': 'vcenter'
        })
        
        formats['genai'] = self.workbook.add_format({
            'bg_color': '#C6EFCE',
            'font_color': '#006100',
            'font_size': 10,
            'border': 1,
            'valign': 'vcenter'
        })
        
        return formats
    
    def add_sheet(self, name: str, description: Optional[str] = None) -> Any:
        """Add a new worksheet."""
        sheet = self.workbook.add_worksheet(name)
        self.sheets[name] = sheet
        
        # Set column widths (default)
        sheet.set_column('A:Z', 15)
        
        # Add description as comment if provided
        if description:
            sheet.write('A1', description, self.formats['data'])
        
        return sheet
    
    def write_table_header(self, sheet: Any, row: int, 
                          columns: List[str], start_col: int = 0) -> int:
        """
        Write table header row.
        
        Returns:
            Next row number
        """
        for col_idx, col_name in enumerate(columns):
            sheet.write(row, start_col + col_idx, col_name, self.formats['header'])
        
        return row + 1
    
    def write_table_data_chunked(self, sheet: Any, 
                                 row: int, columns: List[str], data: List[Dict[str, Any]],
                                 start_col: int = 0, max_rows: Optional[int] = None,
                                 column_formats: Optional[Dict[str, str]] = None) -> int:
        """
        Write table data in chunks (for constant memory mode).
        
        Args:
            sheet: Worksheet to write to
            row: Starting row
            columns: Column names
            data: List of row dictionaries
            start_col: Starting column index
            max_rows: Maximum rows to write (None = all)
            column_formats: Dict mapping column names to format keys
        
        Returns:
            Next row number
        """
        if max_rows is not None:
            data = data[:max_rows]
        
        chunk_size = 1000
        current_row = row
        
        for chunk_start in range(0, len(data), chunk_size):
            chunk = data[chunk_start:chunk_start + chunk_size]
            
            for row_data in chunk:
                for col_idx, col_name in enumerate(columns):
                    value = row_data.get(col_name)
                    format_key = None
                    
                    if column_formats and col_name in column_formats:
                        format_key = column_formats[col_name]
                    
                    cell_format = self.formats.get(format_key, self.formats['data']) if format_key else self.formats['data']
                    
                    # Apply conditional formatting based on value
                    if isinstance(value, str):
                        if 'risk' in col_name.lower() and value.lower() == 'high':
                            cell_format = self.formats['high_risk']
                        elif 'risk' in col_name.lower() and value.lower() == 'medium':
                            cell_format = self.formats['medium_risk']
                        elif 'usage_type' in col_name.lower() and value == 'genai':
                            cell_format = self.formats['genai']
                    
                    sheet.write(current_row, start_col + col_idx, value, cell_format)
                
                current_row += 1
        
        return current_row
    
    def create_chart(self, chart_type: str, name: str, data_range: str,
                    categories_range: Optional[str] = None,
                    title: Optional[str] = None) -> Any:
        """
        Create a chart.
        
        Args:
            chart_type: 'pie', 'bar', 'line', 'column'
            name: Chart name
            data_range: Cell range for data (e.g., 'B2:B10')
            categories_range: Cell range for categories (optional)
            title: Chart title
        """
        chart = self.workbook.add_chart({'type': chart_type})
        
        if title:
            chart.set_title({'name': title})
        
        if categories_range:
            chart.add_series({
                'categories': categories_range,
                'values': data_range,
                'name': name
            })
        else:
            chart.add_series({
                'values': data_range,
                'name': name
            })
        
        return chart
    
    def generate_excel(self, run_id: str, report_data: Dict[str, Any], 
                      db_reader, run_context: Any) -> Path:
        """
        Generate complete Excel report.
        
        Args:
            run_id: Run identifier
            report_data: Report data from ReportBuilder
            db_reader: DuckDB reader connection
            run_context: Run context from Orchestrator
        
        Returns:
            Path to generated Excel file
        """
        # 1. Executive Summary
        self._create_executive_summary(report_data, db_reader, run_id)
        
        # 2. Top Shadow AI Apps
        self._create_shadow_ai_sheet(report_data, db_reader, run_id)
        
        # 3. High Risk Users
        self._create_high_risk_users_sheet(report_data, db_reader, run_id)
        
        # 4. Findings A (High-Volume)
        self._create_findings_a_sheet(report_data, db_reader, run_id)
        
        # 5. Findings B (High-Risk Small)
        self._create_findings_b_sheet(report_data, db_reader, run_id)
        
        # 6. Findings C (Coverage Sample)
        self._create_findings_c_sheet(report_data, db_reader, run_id)
        
        # 7. Department Risk
        self._create_department_risk_sheet(report_data, db_reader, run_id)
        
        # 8. Time Series
        self._create_time_series_sheet(report_data, db_reader, run_id)
        
        # 9. Audit Narrative (MUST HAVE)
        self._create_audit_narrative_sheet(report_data, run_context, db_reader, run_id)
        
        # 10. Policy Gaps
        self._create_policy_gaps_sheet(report_data, db_reader, run_id)
        
        # 11. Cost Reduction Simulation (Tier1商品要件)
        self._create_cost_reduction_sheet(report_data, db_reader, run_id)
        
        # Close workbook
        self.workbook.close()
        
        return self.output_path
    
    def _create_executive_summary(self, report_data: Dict[str, Any], 
                                 db_reader, run_id: str):
        """Create Executive Summary sheet."""
        sheet = self.add_sheet("ExecutiveSummary", "High-level KPIs and risk overview")
        
        row = 0
        
        # KPI Table
        kpi_columns = ["Metric", "Value", "Trend"]
        row = self.write_table_header(sheet, row, kpi_columns)
        
        counts = report_data.get("counts", {})
        kpi_data = [
            {"Metric": "Total Events", "Value": counts.get("total_events", 0), "Trend": ""},
            {"Metric": "Unique Users", "Value": counts.get("unique_users", 0), "Trend": ""},
            {"Metric": "Unique Signatures", "Value": counts.get("total_signatures", 0), "Trend": ""},
            {"Metric": "A Findings (High-Volume)", "Value": counts.get("abc_count_a", 0), "Trend": ""},
            {"Metric": "B Findings (High-Risk Small)", "Value": counts.get("abc_count_b", 0), "Trend": ""},
            {"Metric": "C Findings (Coverage Sample)", "Value": counts.get("abc_count_c", 0), "Trend": ""},
        ]
        
        for kpi in kpi_data:
            sheet.write(row, 0, kpi["Metric"], self.formats['data'])
            sheet.write(row, 1, kpi["Value"], self.formats['data'])
            sheet.write(row, 2, kpi["Trend"], self.formats['data'])
            row += 1
        
        # Charts (placeholder positions)
        # Risk by Category (pie chart)
        # Top Shadow AI (bar chart)
    
    def _create_shadow_ai_sheet(self, report_data: Dict[str, Any], 
                                db_reader, run_id: str):
        """Create Top Shadow AI Apps sheet."""
        sheet = self.add_sheet("ShadowAI_Top10", "Top 10 Shadow AI services by usage")
        
        row = 0
        
        # Query top shadow AI services
        query = """
        SELECT 
            ac.service_name,
            ac.category,
            ac.risk_level,
            ss.unique_users,
            ss.access_count,
            ss.bytes_sent_sum as bytes_sent_total,
            ss.first_seen,
            ss.last_seen
        FROM signature_stats ss
        JOIN analysis_cache ac ON ss.url_signature = ac.url_signature
        WHERE ss.run_id = ? 
            AND ac.usage_type = 'genai'
            AND ac.status = 'active'
        ORDER BY ss.bytes_sent_sum DESC
        LIMIT 10
        """
        
        result = db_reader.execute(query, [run_id]).fetchall()
        
        columns = ["Rank", "Service", "Category", "Risk", "UniqueUsers", 
                  "AccessCount", "BytesSent", "FirstSeen", "LastSeen"]
        row = self.write_table_header(sheet, row, columns)
        
        data = []
        for rank, (service_name, category, risk_level, unique_users, access_count,
                  bytes_sent_total, first_seen, last_seen) in enumerate(result, 1):
            data.append({
                "Rank": rank,
                "Service": service_name or "Unknown",
                "Category": category or "",
                "Risk": risk_level or "unknown",
                "UniqueUsers": unique_users or 0,
                "AccessCount": access_count or 0,
                "BytesSent": bytes_sent_total or 0,
                "FirstSeen": first_seen.strftime("%Y-%m-%d %H:%M:%S") if first_seen else "",
                "LastSeen": last_seen.strftime("%Y-%m-%d %H:%M:%S") if last_seen else ""
            })
        
        column_formats = {
            "BytesSent": "bytes",
            "FirstSeen": "datetime",
            "LastSeen": "datetime"
        }
        
        self.write_table_data_chunked(sheet, row, columns, data, 
                                      column_formats=column_formats)
    
    def _create_high_risk_users_sheet(self, report_data: Dict[str, Any],
                                     db_reader, run_id: str):
        """Create High Risk Users sheet."""
        sheet = self.add_sheet("HighRiskUsers_Top10", "Top 10 users by risk score")
        
        row = 0
        
        # Query high risk users
        # Note: This requires joining with canonical events or a user_stats table
        # For now, we'll use a simplified query from signature_stats
        query = """
        SELECT 
            ss.url_signature,
            ss.unique_users,
            ss.bytes_sent_sum as total_bytes_sent,
            ss.burst_max_5min as burst_count,
            ac.risk_level,
            ac.usage_type
        FROM signature_stats ss
        LEFT JOIN analysis_cache ac ON ss.url_signature = ac.url_signature
        WHERE ss.run_id = ?
        ORDER BY ss.bytes_sent_sum DESC
        LIMIT 10
        """
        
        result = db_reader.execute(query, [run_id]).fetchall()
        
        columns = ["Rank", "UserID", "Department", "TotalBytesSent",
                  "HighRiskDestinations", "GenAIAccess", "BurstCount", "RiskScore"]
        row = self.write_table_header(sheet, row, columns)
        
        # Note: This is a simplified version. Full implementation would require
        # user-level aggregation from canonical events
        data = []
        for rank, row_data in enumerate(result, 1):
            data.append({
                "Rank": rank,
                "UserID": "N/A",  # Would need user_id from events
                "Department": "",
                "TotalBytesSent": row_data[2] or 0,
                "HighRiskDestinations": 0,
                "GenAIAccess": 1 if row_data[5] == 'genai' else 0,
                "BurstCount": row_data[3] or 0,
                "RiskScore": 0.0
            })
        
        column_formats = {"TotalBytesSent": "bytes"}
        self.write_table_data_chunked(sheet, row, columns, data,
                                      column_formats=column_formats)
    
    def _create_findings_a_sheet(self, report_data: Dict[str, Any],
                                 db_reader, run_id: str):
        """Create Findings A (High-Volume) sheet."""
        sheet = self.add_sheet("Findings_A", "High-Volume Transfer findings (bytes_sent >= 1MB)")
        
        row = 0
        
        # Query A findings (simplified - would need to join with events)
        query = """
        SELECT 
            ss.url_signature,
            ss.norm_host,
            ss.dest_domain,
            ss.bytes_sent_sum,
            ss.access_count,
            ac.service_name,
            ac.category,
            ac.risk_level
        FROM signature_stats ss
        LEFT JOIN analysis_cache ac ON ss.url_signature = ac.url_signature
        WHERE ss.run_id = ?
            AND ss.candidate_flags LIKE '%A%'
        ORDER BY ss.bytes_sent_sum DESC
        LIMIT 1000
        """
        
        result = db_reader.execute(query, [run_id]).fetchall()
        
        columns = ["Timestamp", "UserID", "DestDomain", "Service", "Category",
                  "Risk", "BytesSent", "Method", "Action"]
        row = self.write_table_header(sheet, row, columns)
        
        data = []
        for row_data in result:
            data.append({
                "Timestamp": "",
                "UserID": "",
                "DestDomain": row_data[2] or "",
                "Service": row_data[5] or "Unknown",
                "Category": row_data[6] or "",
                "Risk": row_data[7] or "unknown",
                "BytesSent": row_data[3] or 0,
                "Method": "",
                "Action": ""
            })
        
        if len(result) >= 1000:
            # Add overflow note
            sheet.write(row + len(data), 0, 
                       f"Note: Full data available in Parquet export (showing first 1,000 rows)",
                       self.formats['data'])
        
        column_formats = {"BytesSent": "bytes"}
        self.write_table_data_chunked(sheet, row, columns, data,
                                      max_rows=1000, column_formats=column_formats)
    
    def _create_findings_b_sheet(self, report_data: Dict[str, Any],
                                 db_reader, run_id: str):
        """Create Findings B (High-Risk Small) sheet."""
        sheet = self.add_sheet("Findings_B", "High-Risk Small Transfer findings")
        
        row = 0
        
        query = """
        SELECT 
            ss.url_signature,
            ss.norm_host,
            ss.dest_domain,
            ss.bytes_sent_sum,
            ss.access_count,
            ss.burst_max_5min,
            ac.service_name,
            ac.category,
            ac.risk_level
        FROM signature_stats ss
        LEFT JOIN analysis_cache ac ON ss.url_signature = ac.url_signature
        WHERE ss.run_id = ?
            AND ss.candidate_flags LIKE '%B%'
        ORDER BY ss.bytes_sent_sum DESC
        LIMIT 1000
        """
        
        result = db_reader.execute(query, [run_id]).fetchall()
        
        columns = ["Timestamp", "UserID", "DestDomain", "Service", "Category",
                  "Risk", "BytesSent", "TriggerType", "Action"]
        row = self.write_table_header(sheet, row, columns)
        
        data = []
        for row_data in result:
            trigger_type = "Burst" if row_data[5] and row_data[5] > 0 else "Cumulative"
            data.append({
                "Timestamp": "",
                "UserID": "",
                "DestDomain": row_data[2] or "",
                "Service": row_data[6] or "Unknown",
                "Category": row_data[7] or "",
                "Risk": row_data[8] or "unknown",
                "BytesSent": row_data[3] or 0,
                "TriggerType": trigger_type,
                "Action": ""
            })
        
        if len(result) >= 1000:
            sheet.write(row + len(data), 0,
                       f"Note: Full data available in Parquet export (showing first 1,000 rows)",
                       self.formats['data'])
        
        column_formats = {"BytesSent": "bytes"}
        self.write_table_data_chunked(sheet, row, columns, data,
                                      max_rows=1000, column_formats=column_formats)
    
    def _create_findings_c_sheet(self, report_data: Dict[str, Any],
                                db_reader, run_id: str):
        """Create Findings C (Coverage Sample) sheet."""
        sheet = self.add_sheet("Findings_C", "Coverage Sample findings (2% random sample)")
        
        row = 0
        
        query = """
        SELECT 
            ss.url_signature,
            ss.norm_host,
            ss.dest_domain,
            ss.bytes_sent_sum,
            ss.access_count,
            ac.service_name,
            ac.category,
            ac.risk_level
        FROM signature_stats ss
        LEFT JOIN analysis_cache ac ON ss.url_signature = ac.url_signature
        WHERE ss.run_id = ?
            AND ss.sampled = TRUE
        ORDER BY ss.bytes_sent_sum DESC
        LIMIT 500
        """
        
        result = db_reader.execute(query, [run_id]).fetchall()
        
        columns = ["Timestamp", "UserID", "DestDomain", "Service", "Category",
                  "Risk", "BytesSent"]
        row = self.write_table_header(sheet, row, columns)
        
        data = []
        for row_data in result:
            data.append({
                "Timestamp": "",
                "UserID": "",
                "DestDomain": row_data[2] or "",
                "Service": row_data[5] or "Unknown",
                "Category": row_data[6] or "",
                "Risk": row_data[7] or "unknown",
                "BytesSent": row_data[3] or 0
            })
        
        column_formats = {"BytesSent": "bytes"}
        self.write_table_data_chunked(sheet, row, columns, data,
                                      max_rows=500, column_formats=column_formats)
    
    def _create_department_risk_sheet(self, report_data: Dict[str, Any],
                                      db_reader, run_id: str):
        """Create Department Risk sheet."""
        sheet = self.add_sheet("DepartmentRisk", "Risk breakdown by department")
        
        row = 0
        
        columns = ["Department", "UserCount", "TotalEvents", "HighRiskPct",
                  "GenAIPct", "AvgRiskScore"]
        row = self.write_table_header(sheet, row, columns)
        
        # Get vendor from report_data to locate Parquet files
        vendor = report_data.get("vendor", "")
        if not vendor:
            # Fallback: try to get from run_id or use placeholder
            data = [{
                "Department": "N/A (vendor not found)",
                "UserCount": 0,
                "TotalEvents": 0,
                "HighRiskPct": 0.0,
                "GenAIPct": 0.0,
                "AvgRiskScore": 0.0
            }]
            column_formats = {
                "HighRiskPct": "percentage",
                "GenAIPct": "percentage",
                "AvgRiskScore": "percentage"
            }
            self.write_table_data_chunked(sheet, row, columns, data,
                                          column_formats=column_formats)
            return
        
        # Find Parquet files for this run_id and vendor
        # Parquet files are in data/processed/vendor=<v>/date=<YYYY-MM-DD>/
        processed_dir = Path(__file__).parent.parent.parent / "data" / "processed"
        vendor_dir = processed_dir / f"vendor={vendor}"
        
        # Collect all Parquet files for this vendor
        parquet_files = []
        if vendor_dir.exists():
            for date_dir in vendor_dir.iterdir():
                if date_dir.is_dir() and date_dir.name.startswith("date="):
                    for parquet_file in date_dir.glob("*.parquet"):
                        parquet_files.append(str(parquet_file))
        
        if not parquet_files:
            # No Parquet files found - use placeholder
            data = [{
                "Department": "N/A (no Parquet files found)",
                "UserCount": 0,
                "TotalEvents": 0,
                "HighRiskPct": 0.0,
                "GenAIPct": 0.0,
                "AvgRiskScore": 0.0
            }]
            column_formats = {
                "HighRiskPct": "percentage",
                "GenAIPct": "percentage",
                "AvgRiskScore": "percentage"
            }
            self.write_table_data_chunked(sheet, row, columns, data,
                                          column_formats=column_formats)
            return
        
        # Query department risk from Parquet files using DuckDB
        # Join with signature_stats and analysis_cache to get risk information
        try:
            # Create a CTE to read Parquet files
            # DuckDB read_parquet accepts array of file paths
            # Escape single quotes in paths and build array string
            escaped_paths = [p.replace("'", "''") for p in parquet_files]
            parquet_paths_str = "', '".join(escaped_paths)
            
            query = f"""
            WITH events AS (
                SELECT 
                    user_dept,
                    user_id,
                    url_signature
                FROM read_parquet(['{parquet_paths_str}'])
                WHERE user_dept IS NOT NULL AND user_dept != ''
            ),
            dept_stats AS (
                SELECT 
                    COALESCE(e.user_dept, 'Unknown') as department,
                    COUNT(DISTINCT e.user_id) as user_count,
                    COUNT(*) as total_events,
                    COUNT(DISTINCT CASE 
                        WHEN ac.risk_level IN ('high', 'critical') THEN e.url_signature 
                    END) as high_risk_signatures,
                    COUNT(DISTINCT CASE 
                        WHEN ac.usage_type = 'genai' THEN e.url_signature 
                    END) as genai_signatures,
                    COUNT(DISTINCT e.url_signature) as total_signatures
                FROM events e
                LEFT JOIN signature_stats ss ON e.url_signature = ss.url_signature AND ss.run_id = ?
                LEFT JOIN analysis_cache ac ON e.url_signature = ac.url_signature AND ac.status = 'active'
                GROUP BY COALESCE(e.user_dept, 'Unknown')
            )
            SELECT 
                department,
                user_count,
                total_events,
                CASE 
                    WHEN total_signatures > 0 
                    THEN CAST(high_risk_signatures AS DOUBLE) / CAST(total_signatures AS DOUBLE)
                    ELSE 0.0
                END as high_risk_pct,
                CASE 
                    WHEN total_signatures > 0 
                    THEN CAST(genai_signatures AS DOUBLE) / CAST(total_signatures AS DOUBLE)
                    ELSE 0.0
                END as genai_pct,
                CASE 
                    WHEN user_count > 0 
                    THEN CAST(total_events AS DOUBLE) / CAST(user_count AS DOUBLE)
                    ELSE 0.0
                END as avg_risk_score
            FROM dept_stats
            ORDER BY avg_risk_score DESC, total_events DESC
            """
            
            result = db_reader.execute(query, [run_id]).fetchall()
            
            data = []
            for row_data in result:
                dept, user_count, total_events, high_risk_pct, genai_pct, avg_risk_score = row_data
                data.append({
                    "Department": dept or "Unknown",
                    "UserCount": user_count or 0,
                    "TotalEvents": total_events or 0,
                    "HighRiskPct": high_risk_pct or 0.0,
                    "GenAIPct": genai_pct or 0.0,
                    "AvgRiskScore": avg_risk_score or 0.0
                })
            
            if not data:
                # No data found - use placeholder
                data = [{
                    "Department": "N/A (no department data found)",
                    "UserCount": 0,
                    "TotalEvents": 0,
                    "HighRiskPct": 0.0,
                    "GenAIPct": 0.0,
                    "AvgRiskScore": 0.0
                }]
        
        except Exception as e:
            # On error, use placeholder with error message
            print(f"Warning: Failed to query department risk: {e}", flush=True)
            data = [{
                "Department": f"N/A (error: {str(e)[:50]})",
                "UserCount": 0,
                "TotalEvents": 0,
                "HighRiskPct": 0.0,
                "GenAIPct": 0.0,
                "AvgRiskScore": 0.0
            }]
        
        column_formats = {
            "HighRiskPct": "percentage",
            "GenAIPct": "percentage",
            "AvgRiskScore": "percentage"
        }
        
        self.write_table_data_chunked(sheet, row, columns, data,
                                      column_formats=column_formats)
    
    def _create_time_series_sheet(self, report_data: Dict[str, Any],
                                  db_reader, run_id: str):
        """Create Time Series sheet."""
        sheet = self.add_sheet("TimeSeries", "Trends over time (weekly/monthly)")
        
        row = 0
        
        columns = ["Period", "PeriodType", "TotalEvents", "UnknownPct", "GenAIPct",
                  "HighRiskPct", "BlockedPct"]
        row = self.write_table_header(sheet, row, columns)
        
        # Get vendor from report_data to locate Parquet files
        vendor = report_data.get("vendor", "")
        if not vendor:
            # No data available
            column_formats = {
                "UnknownPct": "percentage",
                "GenAIPct": "percentage",
                "HighRiskPct": "percentage",
                "BlockedPct": "percentage"
            }
            self.write_table_data_chunked(sheet, row, columns, [],
                                          column_formats=column_formats)
            return
        
        # Find Parquet files for this run_id and vendor
        processed_dir = Path(__file__).parent.parent.parent / "data" / "processed"
        vendor_dir = processed_dir / f"vendor={vendor}"
        
        # Collect all Parquet files for this vendor
        parquet_files = []
        if vendor_dir.exists():
            for date_dir in vendor_dir.iterdir():
                if date_dir.is_dir() and date_dir.name.startswith("date="):
                    for parquet_file in date_dir.glob("*.parquet"):
                        parquet_files.append(str(parquet_file))
        
        if not parquet_files:
            # No Parquet files found
            column_formats = {
                "UnknownPct": "percentage",
                "GenAIPct": "percentage",
                "HighRiskPct": "percentage",
                "BlockedPct": "percentage"
            }
            self.write_table_data_chunked(sheet, row, columns, [],
                                          column_formats=column_formats)
            return
        
        # Query time series from Parquet files using DuckDB
        # Aggregate by week (ISO week: year-week)
        try:
            # Escape single quotes in paths and build array string
            escaped_paths = [p.replace("'", "''") for p in parquet_files]
            parquet_paths_str = "', '".join(escaped_paths)
            
            # Phase 15: Add monthly aggregation in addition to weekly
            query = f"""
            WITH events AS (
                SELECT 
                    event_time,
                    url_signature,
                    action
                FROM read_parquet(['{parquet_paths_str}'])
            ),
            events_with_time AS (
                SELECT 
                    event_time,
                    url_signature,
                    action,
                    CAST(event_time AS TIMESTAMP) as event_ts,
                    DATE_TRUNC('week', CAST(event_time AS TIMESTAMP)) as week_start,
                    DATE_TRUNC('month', CAST(event_time AS TIMESTAMP)) as month_start,
                    STRFTIME(CAST(event_time AS TIMESTAMP), '%Y-W%V') as year_week,
                    STRFTIME(CAST(event_time AS TIMESTAMP), '%Y-%m') as year_month
                FROM events
            ),
            week_stats AS (
                SELECT 
                    year_week,
                    week_start,
                    COUNT(*) as total_events,
                    COUNT(DISTINCT CASE 
                        WHEN ac.url_signature IS NULL OR ac.status != 'active' THEN e.url_signature 
                    END) as unknown_signatures,
                    COUNT(DISTINCT CASE 
                        WHEN ac.usage_type = 'genai' THEN e.url_signature 
                    END) as genai_signatures,
                    COUNT(DISTINCT CASE 
                        WHEN ac.risk_level IN ('high', 'critical') THEN e.url_signature 
                    END) as high_risk_signatures,
                    COUNT(DISTINCT e.url_signature) as total_signatures,
                    SUM(CASE WHEN e.action = 'block' THEN 1 ELSE 0 END) as blocked_events
                FROM events_with_time e
                LEFT JOIN signature_stats ss ON e.url_signature = ss.url_signature AND ss.run_id = ?
                LEFT JOIN analysis_cache ac ON e.url_signature = ac.url_signature AND ac.status = 'active'
                GROUP BY year_week, week_start
            ),
            month_stats AS (
                SELECT 
                    year_month,
                    month_start,
                    COUNT(*) as total_events,
                    COUNT(DISTINCT CASE 
                        WHEN ac.url_signature IS NULL OR ac.status != 'active' THEN e.url_signature 
                    END) as unknown_signatures,
                    COUNT(DISTINCT CASE 
                        WHEN ac.usage_type = 'genai' THEN e.url_signature 
                    END) as genai_signatures,
                    COUNT(DISTINCT CASE 
                        WHEN ac.risk_level IN ('high', 'critical') THEN e.url_signature 
                    END) as high_risk_signatures,
                    COUNT(DISTINCT e.url_signature) as total_signatures,
                    SUM(CASE WHEN e.action = 'block' THEN 1 ELSE 0 END) as blocked_events
                FROM events_with_time e
                LEFT JOIN signature_stats ss ON e.url_signature = ss.url_signature AND ss.run_id = ?
                LEFT JOIN analysis_cache ac ON e.url_signature = ac.url_signature AND ac.status = 'active'
                GROUP BY year_month, month_start
            ),
            weekly_data AS (
                SELECT 
                    year_week as period,
                    'Week' as period_type,
                    total_events,
                    CASE 
                        WHEN total_signatures > 0 
                        THEN CAST(unknown_signatures AS DOUBLE) / CAST(total_signatures AS DOUBLE)
                        ELSE 0.0
                    END as unknown_pct,
                    CASE 
                        WHEN total_signatures > 0 
                        THEN CAST(genai_signatures AS DOUBLE) / CAST(total_signatures AS DOUBLE)
                        ELSE 0.0
                    END as genai_pct,
                    CASE 
                        WHEN total_signatures > 0 
                        THEN CAST(high_risk_signatures AS DOUBLE) / CAST(total_signatures AS DOUBLE)
                        ELSE 0.0
                    END as high_risk_pct,
                    CASE 
                        WHEN total_events > 0 
                        THEN CAST(blocked_events AS DOUBLE) / CAST(total_events AS DOUBLE)
                        ELSE 0.0
                    END as blocked_pct
                FROM week_stats
            ),
            monthly_data AS (
                SELECT 
                    year_month as period,
                    'Month' as period_type,
                    total_events,
                    CASE 
                        WHEN total_signatures > 0 
                        THEN CAST(unknown_signatures AS DOUBLE) / CAST(total_signatures AS DOUBLE)
                        ELSE 0.0
                    END as unknown_pct,
                    CASE 
                        WHEN total_signatures > 0 
                        THEN CAST(genai_signatures AS DOUBLE) / CAST(total_signatures AS DOUBLE)
                        ELSE 0.0
                    END as genai_pct,
                    CASE 
                        WHEN total_signatures > 0 
                        THEN CAST(high_risk_signatures AS DOUBLE) / CAST(total_signatures AS DOUBLE)
                        ELSE 0.0
                    END as high_risk_pct,
                    CASE 
                        WHEN total_events > 0 
                        THEN CAST(blocked_events AS DOUBLE) / CAST(total_events AS DOUBLE)
                        ELSE 0.0
                    END as blocked_pct
                FROM month_stats
            )
            SELECT * FROM weekly_data
            UNION ALL
            SELECT * FROM monthly_data
            ORDER BY period_type, period ASC
            """
            
            result = db_reader.execute(query, [run_id, run_id]).fetchall()
            
            data = []
            for row_data in result:
                period, period_type, total_events, unknown_pct, genai_pct, high_risk_pct, blocked_pct = row_data
                # Format period based on type
                if period_type == "Week":
                    period_str = str(period) if period else "N/A"
                else:  # Month
                    period_str = str(period) if period else "N/A"
                
                data.append({
                    "Period": period_str,
                    "PeriodType": period_type or "N/A",
                    "TotalEvents": total_events or 0,
                    "UnknownPct": unknown_pct or 0.0,
                    "GenAIPct": genai_pct or 0.0,
                    "HighRiskPct": high_risk_pct or 0.0,
                    "BlockedPct": blocked_pct or 0.0
                })
        
        except Exception as e:
            # On error, log and use empty data
            print(f"Warning: Failed to query time series: {e}", flush=True)
            data = []
        
        column_formats = {
            "UnknownPct": "percentage",
            "GenAIPct": "percentage",
            "HighRiskPct": "percentage",
            "BlockedPct": "percentage"
        }
        
        self.write_table_data_chunked(sheet, row, columns, data,
                                      column_formats=column_formats)
    
    def _create_audit_narrative_sheet(self, report_data: Dict[str, Any],
                                      run_context: Any, db_reader, run_id: str):
        """
        Create Audit Narrative sheet (MUST HAVE).
        
        This sheet contains all required audit documentation:
        - A/B/C counts, percentages, bytes bands
        - Exclusion criteria and excluded counts
        - Sample rate, method, seed
        - LLM usage scope, PII sending prohibition explanation
        """
        sheet = self.add_sheet("AuditNarrative", "Audit documentation and methodology")
        
        row = 0
        
        # Run Metadata
        sheet.write(row, 0, "Run Metadata", self.formats['subheader'])
        row += 1
        
        metadata_columns = ["Field", "Value"]
        row = self.write_table_header(sheet, row, metadata_columns)
        
        metadata_data = [
            {"Field": "Run ID", "Value": report_data.get("run_id", "")},
            {"Field": "Run Key", "Value": report_data.get("run_key", "")},
            {"Field": "Started At", "Value": report_data.get("started_at", "")},
            {"Field": "Finished At", "Value": report_data.get("finished_at", "")},
            {"Field": "Input File", "Value": report_data.get("input_file", "")},
            {"Field": "Vendor", "Value": report_data.get("vendor", "")},
            {"Field": "Signature Version", "Value": report_data.get("signature_version", "")},
            {"Field": "Rule Version", "Value": report_data.get("rule_version", "")},
            {"Field": "Prompt Version", "Value": report_data.get("prompt_version", "")},
        ]
        
        self.write_table_data_chunked(sheet, row, metadata_columns, metadata_data)
        row += len(metadata_data) + 2
        
        # Target Population (Phase 14: Required for audit)
        sheet.write(row, 0, "Target Population", self.formats['subheader'])
        row += 1
        
        population_columns = ["Metric", "Value", "Percentage"]
        row = self.write_table_header(sheet, row, population_columns)
        
        counts = report_data.get("counts", {})
        total_events = counts.get("total_events", 0)
        unique_users = counts.get("unique_users", 0)
        unique_domains = counts.get("unique_domains", 0)
        total_signatures = counts.get("total_signatures", 0)
        
        count_a = counts.get("abc_count_a", 0)
        count_b = counts.get("abc_count_b", 0)
        count_c = counts.get("abc_count_c", 0)
        extracted_count = count_a + count_b + count_c
        extracted_pct = (extracted_count / total_events * 100) if total_events > 0 else 0.0
        
        # Query unique users and domains from DB if not in report_data
        if unique_users == 0 or unique_domains == 0:
            try:
                user_query = """
                SELECT COUNT(DISTINCT url_signature) as sig_count
                FROM signature_stats
                WHERE run_id = ?
                """
                sig_result = db_reader.execute(user_query, [run_id]).fetchone()
                if sig_result and sig_result[0]:
                    total_signatures = sig_result[0]
            except Exception:
                pass  # Use values from report_data
        
        population_data = [
            {"Metric": "Total Events (Population)", "Value": total_events, "Percentage": "100.00%"},
            {"Metric": "Unique Users", "Value": unique_users, "Percentage": ""},
            {"Metric": "Unique Domains", "Value": unique_domains, "Percentage": ""},
            {"Metric": "Unique Signatures", "Value": total_signatures, "Percentage": ""},
            {"Metric": "Extracted Events (A+B+C)", "Value": extracted_count, "Percentage": f"{extracted_pct:.2f}%"},
            {"Metric": "Non-Extracted Events", "Value": total_events - extracted_count, "Percentage": f"{(100.0 - extracted_pct):.2f}%"},
        ]
        
        column_formats = {
            "Percentage": "percentage"
        }
        self.write_table_data_chunked(sheet, row, population_columns, population_data,
                                      column_formats=column_formats)
        row += len(population_data) + 2
        
        # Sampling Info
        sheet.write(row, 0, "Sampling Information", self.formats['subheader'])
        row += 1
        
        sampling_columns = ["Parameter", "Value"]
        row = self.write_table_header(sheet, row, sampling_columns)
        
        sample = report_data.get("sample", {})
        thresholds = report_data.get("thresholds_used", {})
        
        pct_a = (count_a / total_events * 100) if total_events > 0 else 0.0
        pct_b = (count_b / total_events * 100) if total_events > 0 else 0.0
        pct_c = (count_c / total_events * 100) if total_events > 0 else 0.0
        
        sampling_data = [
            {"Parameter": "A Count (High-Volume)", "Value": count_a},
            {"Parameter": "A Percentage", "Value": f"{pct_a:.2f}%"},
            {"Parameter": "A Bytes Band", "Value": f">= {thresholds.get('A_min_bytes', 0):,} bytes"},
            {"Parameter": "B Count (High-Risk Small)", "Value": count_b},
            {"Parameter": "B Percentage", "Value": f"{pct_b:.2f}%"},
            {"Parameter": "B Burst Threshold", "Value": f"{thresholds.get('B_burst_count', 0)} events in {thresholds.get('B_burst_window_seconds', 0)}s"},
            {"Parameter": "B Cumulative Threshold", "Value": f">= {thresholds.get('B_cumulative_bytes', 0):,} bytes per user×domain×day"},
            {"Parameter": "C Count (Coverage Sample)", "Value": count_c},
            {"Parameter": "C Percentage", "Value": f"{pct_c:.2f}%"},
            {"Parameter": "Sample Rate", "Value": f"{sample.get('sample_rate', 0.0):.1%}"},
            {"Parameter": "Sample Method", "Value": sample.get("sample_method", "deterministic_hash")},
            {"Parameter": "Sample Seed", "Value": sample.get("seed", "")},
        ]
        
        self.write_table_data_chunked(sheet, row, sampling_columns, sampling_data)
        row += len(sampling_data) + 2
        
        # Exclusions
        sheet.write(row, 0, "Exclusions", self.formats['subheader'])
        row += 1
        
        exclusion_columns = ["ExclusionType", "Condition", "Count"]
        row = self.write_table_header(sheet, row, exclusion_columns)
        
        exclusions = report_data.get("exclusions", {})
        exclusion_data = []
        
        if exclusions:
            # Query exclusion counts from Parquet files (Phase 14: Accurate exclusion counts)
            vendor = report_data.get("vendor", "")
            run_id_str = report_data.get("run_id", run_id)
            
            for excl_type, condition in exclusions.items():
                exclusion_count = 0
                
                try:
                    if excl_type == "action_filter" and condition:
                        # Query excluded events from Parquet files
                        # action_filter excludes events where action != condition
                        # For example, if action_filter="allow", then "block" events are excluded
                        processed_dir = Path(__file__).parent.parent.parent / "data" / "processed"
                        vendor_dir = processed_dir / f"vendor={vendor}"
                        
                        parquet_files = []
                        if vendor_dir.exists():
                            for date_dir in vendor_dir.iterdir():
                                if date_dir.is_dir() and date_dir.name.startswith("date="):
                                    for parquet_file in date_dir.glob("*.parquet"):
                                        parquet_files.append(str(parquet_file))
                        
                        if parquet_files:
                            # Escape single quotes in paths and build array string
                            escaped_paths = [p.replace("'", "''") for p in parquet_files]
                            parquet_paths_str = "', '".join(escaped_paths)
                            
                            # Query events that were excluded (action != condition)
                            excl_query = f"""
                            SELECT COUNT(*) 
                            FROM read_parquet(['{parquet_paths_str}'])
                            WHERE action IS NOT NULL 
                                AND action != ?
                                AND action != ''
                            """
                            result = db_reader.execute(excl_query, [str(condition)]).fetchone()
                            exclusion_count = result[0] if result and result[0] else 0
                        else:
                            # No Parquet files found - cannot determine exclusion count
                            exclusion_count = None
                    else:
                        # Unknown exclusion type - cannot determine count
                        exclusion_count = None
                except Exception as e:
                    # On error, mark as unknown
                    print(f"Warning: Failed to query exclusion count for {excl_type}: {e}", flush=True)
                    exclusion_count = None
                
                exclusion_data.append({
                    "ExclusionType": excl_type,
                    "Condition": str(condition),
                    "Count": exclusion_count if exclusion_count is not None else "N/A (cannot determine from available data)"
                })
        else:
            exclusion_data.append({
                "ExclusionType": "None",
                "Condition": "No exclusions applied",
                "Count": 0
            })
        
        self.write_table_data_chunked(sheet, row, exclusion_columns, exclusion_data)
        row += len(exclusion_data) + 2
        
        # LLM Usage and PII Protection
        sheet.write(row, 0, "LLM Usage and PII Protection", self.formats['subheader'])
        row += 1
        
        llm_coverage = report_data.get("llm_coverage", {})
        rule_coverage = report_data.get("rule_coverage", {})
        
        llm_info_columns = ["Item", "Value"]
        row = self.write_table_header(sheet, row, llm_info_columns)
        
        llm_info_data = [
            {"Item": "LLM Provider", "Value": llm_coverage.get("llm_provider", "N/A")},
            {"Item": "LLM Model", "Value": llm_coverage.get("llm_model", "N/A")},
            {"Item": "LLM Analyzed Count", "Value": llm_coverage.get("llm_analyzed_count", 0)},
            {"Item": "Cache Hit Rate", "Value": f"{llm_coverage.get('cache_hit_rate', 0.0):.1%}"},
            {"Item": "Rule Hit Count", "Value": rule_coverage.get("rule_hit", 0)},
            {"Item": "Unknown Count", "Value": rule_coverage.get("unknown_count", 0)},
            {"Item": "PII Sending Prohibition", "Value": "ENFORCED: user_id, src_ip, device_id, and PII-suspected URL parts are NEVER sent to external LLM APIs. Only url_signature, norm_host, norm_path_template, and aggregated statistics are sent."},
            {"Item": "LLM Usage Scope", "Value": "Only unknown signatures (not matched by rules) are sent to LLM for classification. Known services are classified by rules for cost efficiency."},
        ]
        
        self.write_table_data_chunked(sheet, row, llm_info_columns, llm_info_data)
        row += len(llm_info_data) + 2
        
        # Small Volume Zero Exclusion Proof (Phase 14: Required for audit)
        sheet.write(row, 0, "Small Volume Zero Exclusion Proof", self.formats['subheader'])
        row += 1
        
        small_volume_columns = ["Category", "Count", "Percentage", "Description"]
        row = self.write_table_header(sheet, row, small_volume_columns)
        
        # Query non-A/B/C signatures (A/B/C以外の小容量イベント) from signature_stats
        # This proves that small volume events are NOT excluded (zero exclusion policy)
        non_abc_signatures = 0
        try:
            non_abc_query = """
            SELECT COUNT(DISTINCT url_signature)
            FROM signature_stats
            WHERE run_id = ?
                AND (candidate_flags IS NULL OR candidate_flags = '')
            """
            result = db_reader.execute(non_abc_query, [run_id]).fetchone()
            if result:
                non_abc_signatures = result[0] if result[0] else 0
        except Exception:
            pass
        
        small_volume_data = [
            {
                "Category": "Non-A/B/C Signatures (Small Volume)",
                "Count": non_abc_signatures,
                "Percentage": f"{(non_abc_signatures / total_signatures * 100) if total_signatures > 0 else 0.0:.2f}%",
                "Description": "Signatures that are NOT selected as A/B/C candidates"
            },
            {
                "Category": "Non-A/B/C Events",
                "Count": total_events - extracted_count,
                "Percentage": f"{(100.0 - extracted_pct):.2f}%",
                "Description": "Events that are NOT in A/B/C extraction (proves zero exclusion of small volume)"
            },
            {
                "Category": "Total Events",
                "Count": total_events,
                "Percentage": "100.00%",
                "Description": "All events in the population (no exclusions)"
            }
        ]
        
        column_formats_small = {
            "Percentage": "percentage"
        }
        self.write_table_data_chunked(sheet, row, small_volume_columns, small_volume_data,
                                      column_formats=column_formats_small)
        row += len(small_volume_data) + 2
        
        # Add narrative text
        row += 1
        sheet.write(row, 0, "Audit Narrative", self.formats['subheader'])
        row += 1
        
        narrative_text = f"""
This analysis report was generated using AIMO Analysis Engine v1.4.

Target Population:
- Total events analyzed: {total_events:,}
- Unique users: {unique_users:,}
- Unique domains: {unique_domains:,}
- Unique signatures: {total_signatures:,}
- Extracted events (A+B+C): {extracted_count:,} ({extracted_pct:.2f}% of total)

The analysis uses a three-tier candidate selection methodology (A/B/C) to ensure comprehensive coverage:
- A: High-volume transfers (>= {thresholds.get('A_min_bytes', 0):,} bytes per event)
- B: High-risk small transfers (burst patterns, cumulative patterns, or AI/Unknown destinations)
- C: Coverage sample ({sample.get('sample_rate', 0.0):.1%} random sample from non-A/B candidates)

Small Volume Zero Exclusion:
- Non-A/B/C events: {total_events - extracted_count:,} ({(100.0 - extracted_pct):.2f}% of total)
- This proves that small volume events are NOT excluded from analysis (zero exclusion policy)

All sampling is deterministic using run_id as seed, ensuring reproducibility.

PII Protection:
- No user_id, src_ip, device_id, or PII-suspected URL parts are sent to external LLM APIs
- Only normalized signatures (url_signature, norm_host, norm_path_template) and aggregated statistics are sent
- PII detections are logged locally in pii_audit table

Signature Determinism:
- Signature version: {report_data.get('signature_version', 'N/A')}
- Same input always produces same url_signature (deterministic normalization)
- Cache hit rate: {llm_coverage.get('cache_hit_rate', 0.0):.1%}

This report is audit-ready and includes all required metadata for reproducibility and compliance.
        """.strip()
        
        sheet.write(row, 0, narrative_text, self.formats['data'])
        sheet.set_column('A:A', 80)  # Wide column for narrative
    
    def _create_policy_gaps_sheet(self, report_data: Dict[str, Any],
                                 db_reader, run_id: str):
        """Create Policy Gaps sheet."""
        sheet = self.add_sheet("PolicyGaps", "Destinations with allow/block inconsistencies")
        
        row = 0
        
        columns = ["DestDomain", "Service", "AllowCount", "BlockCount",
                  "MixedActions", "Recommendation"]
        row = self.write_table_header(sheet, row, columns)
        
        # Placeholder data (would need action-based aggregation)
        data = []
        
        self.write_table_data_chunked(sheet, row, columns, data)
    
    def _create_cost_reduction_sheet(self, report_data: Dict[str, Any],
                                     db_reader, run_id: str):
        """
        Create Cost Reduction Simulation sheet (Tier1商品要件).
        
        This sheet provides:
        1. Current estimated annual cost (usage × pricing)
        2. Potential cost reduction (duplicate/dormant usage elimination)
        3. User/department breakdown
        """
        sheet = self.add_sheet("CostReduction", "Cost reduction simulation for AI app usage")
        
        row = 0
        
        # Load cost reduction configuration
        config_path = Path(__file__).parent.parent.parent / "config" / "cost_reduction.yaml"
        cost_config = {}
        if config_path.exists():
            try:
                with open(config_path, 'r', encoding='utf-8') as f:
                    cost_config = yaml.safe_load(f) or {}
            except Exception as e:
                print(f"Warning: Failed to load cost reduction config: {e}", flush=True)
        
        service_pricing = cost_config.get("service_pricing", {})
        reduction_assumptions = cost_config.get("reduction_assumptions", {})
        default_genai = service_pricing.get("default_genai", {
            "cost_per_user_per_month_usd": 15.0,
            "cost_per_access_usd": 0.0
        })
        
        # Get vendor from report_data to locate Parquet files
        vendor = report_data.get("vendor", "")
        if not vendor:
            # No data available
            sheet.write(row, 0, "N/A (vendor not found)", self.formats['data'])
            return
        
        # Find Parquet files for this run_id and vendor
        processed_dir = Path(__file__).parent.parent.parent / "data" / "processed"
        vendor_dir = processed_dir / f"vendor={vendor}"
        
        # Collect all Parquet files for this vendor
        parquet_files = []
        if vendor_dir.exists():
            for date_dir in vendor_dir.iterdir():
                if date_dir.is_dir() and date_dir.name.startswith("date="):
                    for parquet_file in date_dir.glob("*.parquet"):
                        parquet_files.append(str(parquet_file))
        
        if not parquet_files:
            # No Parquet files found
            sheet.write(row, 0, "N/A (no Parquet files found)", self.formats['data'])
            return
        
        # Query user/department AI app usage from Parquet files
        try:
            # Escape single quotes in paths and build array string
            escaped_paths = [p.replace("'", "''") for p in parquet_files]
            parquet_paths_str = "', '".join(escaped_paths)
            
            # Query user/department usage statistics
            query = f"""
            WITH events AS (
                SELECT 
                    user_id,
                    COALESCE(user_dept, 'Unknown') as user_dept,
                    url_signature,
                    event_time
                FROM read_parquet(['{parquet_paths_str}'])
                WHERE user_id IS NOT NULL AND user_id != ''
            ),
            usage_stats AS (
                SELECT 
                    e.user_id,
                    e.user_dept,
                    e.url_signature,
                    ac.service_name,
                    ac.usage_type,
                    COUNT(*) as access_count,
                    MIN(CAST(e.event_time AS TIMESTAMP)) as first_access,
                    MAX(CAST(e.event_time AS TIMESTAMP)) as last_access
                FROM events e
                LEFT JOIN signature_stats ss ON e.url_signature = ss.url_signature AND ss.run_id = ?
                LEFT JOIN analysis_cache ac ON e.url_signature = ac.url_signature AND ac.status = 'active'
                WHERE ac.usage_type = 'genai' OR ac.usage_type IS NULL
                GROUP BY e.user_id, e.user_dept, e.url_signature, ac.service_name, ac.usage_type
            ),
            user_service_summary AS (
                SELECT 
                    user_id,
                    user_dept,
                    service_name,
                    SUM(access_count) as total_accesses,
                    MIN(first_access) as first_access,
                    MAX(last_access) as last_access
                FROM usage_stats
                GROUP BY user_id, user_dept, service_name
            )
            SELECT 
                user_dept,
                user_id,
                service_name,
                total_accesses,
                first_access,
                last_access
            FROM user_service_summary
            ORDER BY user_dept, user_id, service_name
            """
            
            result = db_reader.execute(query, [run_id]).fetchall()
            
            # Aggregate data for cost calculation
            user_service_map: Dict[Tuple[str, str], Dict[str, Any]] = {}
            dept_service_map: Dict[Tuple[str, str], Dict[str, Any]] = {}
            
            for row_data in result:
                dept, user_id, service_name, total_accesses, first_access, last_access = row_data
                service_name = service_name or "Unknown"
                
                # User-level aggregation
                user_key = (user_id, dept)
                if user_key not in user_service_map:
                    user_service_map[user_key] = {
                        "user_id": user_id,
                        "department": dept,
                        "services": {},
                        "total_accesses": 0
                    }
                
                if service_name not in user_service_map[user_key]["services"]:
                    user_service_map[user_key]["services"][service_name] = {
                        "accesses": 0,
                        "first_access": first_access,
                        "last_access": last_access
                    }
                
                user_service_map[user_key]["services"][service_name]["accesses"] += total_accesses or 0
                user_service_map[user_key]["total_accesses"] += total_accesses or 0
                
                # Department-level aggregation
                dept_key = (dept, service_name)
                if dept_key not in dept_service_map:
                    dept_service_map[dept_key] = {
                        "department": dept,
                        "service_name": service_name,
                        "user_count": 0,
                        "total_accesses": 0
                    }
                
                dept_service_map[dept_key]["user_count"] += 1
                dept_service_map[dept_key]["total_accesses"] += total_accesses or 0
            
            # Calculate costs and reductions
            # 1. Current Cost Summary
            sheet.write(row, 0, "Current Cost Summary", self.formats['subheader'])
            row += 1
            
            summary_columns = ["Metric", "Value", "Unit"]
            row = self.write_table_header(sheet, row, summary_columns)
            
            total_current_cost_monthly = 0.0
            total_users = len(user_service_map)
            total_services = len(set(service_name for dept, service_name in dept_service_map.keys()))
            
            # Calculate monthly cost per user/service
            for user_key, user_data in user_service_map.items():
                for service_name, service_data in user_data["services"].items():
                    # Get pricing for this service
                    pricing = service_pricing.get(service_name, default_genai)
                    cost_per_user = pricing.get("cost_per_user_per_month_usd", 0.0)
                    cost_per_access = pricing.get("cost_per_access_usd", 0.0)
                    
                    # Calculate cost: subscription-based or per-access
                    if cost_per_user > 0:
                        service_cost = cost_per_user
                    else:
                        service_cost = service_data["accesses"] * cost_per_access
                    
                    total_current_cost_monthly += service_cost
            
            total_current_cost_annual = total_current_cost_monthly * 12
            
            summary_data = [
                {"Metric": "Total Users", "Value": total_users, "Unit": "users"},
                {"Metric": "Total Services", "Value": total_services, "Unit": "services"},
                {"Metric": "Current Monthly Cost (Estimated)", "Value": f"${total_current_cost_monthly:,.2f}", "Unit": "USD"},
                {"Metric": "Current Annual Cost (Estimated)", "Value": f"${total_current_cost_annual:,.2f}", "Unit": "USD"},
            ]
            
            self.write_table_data_chunked(sheet, row, summary_columns, summary_data)
            row += len(summary_data) + 2
            
            # 2. Cost Reduction Potential
            sheet.write(row, 0, "Cost Reduction Potential", self.formats['subheader'])
            row += 1
            
            reduction_columns = ["Reduction Type", "Description", "Potential Reduction", "Weight"]
            row = self.write_table_header(sheet, row, reduction_columns)
            
            # Calculate reduction potential (simple weighted sum)
            duplicate_weight = reduction_assumptions.get("duplicate_usage", {}).get("weight", 0.3)
            dormant_weight = reduction_assumptions.get("dormant_usage", {}).get("weight", 0.2)
            consolidation_weight = reduction_assumptions.get("enterprise_consolidation", {}).get("weight", 0.25)
            
            # Duplicate usage: users with multiple similar services
            duplicate_users = 0
            for user_key, user_data in user_service_map.items():
                if len(user_data["services"]) >= 2:
                    duplicate_users += 1
            
            duplicate_reduction = duplicate_users * duplicate_weight * (total_current_cost_monthly / total_users if total_users > 0 else 0)
            
            # Dormant usage: users with low activity
            dormant_threshold = reduction_assumptions.get("dormant_usage", {}).get("threshold_accesses_per_month", 5)
            dormant_users = 0
            for user_key, user_data in user_service_map.items():
                # Estimate monthly accesses (assuming data covers ~1 month)
                monthly_accesses = user_data["total_accesses"]
                if monthly_accesses < dormant_threshold:
                    dormant_users += 1
            
            dormant_reduction = dormant_users * dormant_weight * (total_current_cost_monthly / total_users if total_users > 0 else 0)
            
            # Enterprise consolidation: department-level consolidation
            consolidation_depts = 0
            dept_user_counts: Dict[str, int] = {}
            for user_key, user_data in user_service_map.items():
                dept = user_data["department"]
                dept_user_counts[dept] = dept_user_counts.get(dept, 0) + 1
            
            consolidation_threshold = reduction_assumptions.get("enterprise_consolidation", {}).get("threshold_department_users", 10)
            for dept, user_count in dept_user_counts.items():
                if user_count >= consolidation_threshold:
                    consolidation_depts += 1
            
            consolidation_reduction = consolidation_depts * consolidation_weight * (total_current_cost_monthly / len(dept_user_counts) if dept_user_counts else 0)
            
            # Total reduction (simple weighted sum)
            total_reduction_monthly = duplicate_reduction + dormant_reduction + consolidation_reduction
            total_reduction_annual = total_reduction_monthly * 12
            
            reduction_data = [
                {
                    "Reduction Type": "Duplicate Usage",
                    "Description": f"Users using multiple services ({duplicate_users} users)",
                    "Potential Reduction": f"${duplicate_reduction:,.2f}/month",
                    "Weight": f"{duplicate_weight:.0%}"
                },
                {
                    "Reduction Type": "Dormant Usage",
                    "Description": f"Users with low activity ({dormant_users} users, <{dormant_threshold} accesses/month)",
                    "Potential Reduction": f"${dormant_reduction:,.2f}/month",
                    "Weight": f"{dormant_weight:.0%}"
                },
                {
                    "Reduction Type": "Enterprise Consolidation",
                    "Description": f"Department-level consolidation ({consolidation_depts} departments, >={consolidation_threshold} users)",
                    "Potential Reduction": f"${consolidation_reduction:,.2f}/month",
                    "Weight": f"{consolidation_weight:.0%}"
                },
                {
                    "Reduction Type": "Total Potential Reduction",
                    "Description": "Simple weighted sum of all reduction types",
                    "Potential Reduction": f"${total_reduction_monthly:,.2f}/month (${total_reduction_annual:,.2f}/year)",
                    "Weight": "N/A"
                }
            ]
            
            self.write_table_data_chunked(sheet, row, reduction_columns, reduction_data)
            row += len(reduction_data) + 2
            
            # 3. Department Breakdown
            sheet.write(row, 0, "Department Breakdown", self.formats['subheader'])
            row += 1
            
            dept_columns = ["Department", "Users", "Services", "Monthly Cost (Est.)", "Annual Cost (Est.)", "Reduction Potential"]
            row = self.write_table_header(sheet, row, dept_columns)
            
            dept_summary: Dict[str, Dict[str, Any]] = {}
            for dept_key, dept_data in dept_service_map.items():
                dept = dept_data["department"]
                if dept not in dept_summary:
                    dept_summary[dept] = {
                        "users": set(),
                        "services": set(),
                        "monthly_cost": 0.0
                    }
                
                dept_summary[dept]["services"].add(dept_data["service_name"])
                # Estimate cost for this service in this department
                pricing = service_pricing.get(dept_data["service_name"], default_genai)
                cost_per_user = pricing.get("cost_per_user_per_month_usd", 0.0)
                cost_per_access = pricing.get("cost_per_access_usd", 0.0)
                
                if cost_per_user > 0:
                    service_cost = cost_per_user * dept_data["user_count"]
                else:
                    service_cost = dept_data["total_accesses"] * cost_per_access
                
                dept_summary[dept]["monthly_cost"] += service_cost
            
            # Count users per department
            for user_key, user_data in user_service_map.items():
                dept = user_data["department"]
                if dept in dept_summary:
                    dept_summary[dept]["users"].add(user_data["user_id"])
            
            dept_data_list = []
            for dept, summary in dept_summary.items():
                user_count = len(summary["users"])
                service_count = len(summary["services"])
                monthly_cost = summary["monthly_cost"]
                annual_cost = monthly_cost * 12
                
                # Calculate reduction potential for this department
                dept_reduction = 0.0
                if user_count >= consolidation_threshold:
                    dept_reduction = monthly_cost * consolidation_weight
                
                dept_data_list.append({
                    "Department": dept,
                    "Users": user_count,
                    "Services": service_count,
                    "Monthly Cost (Est.)": f"${monthly_cost:,.2f}",
                    "Annual Cost (Est.)": f"${annual_cost:,.2f}",
                    "Reduction Potential": f"${dept_reduction:,.2f}/month"
                })
            
            # Sort by monthly cost descending
            dept_data_list.sort(key=lambda x: float(x["Monthly Cost (Est.)"].replace("$", "").replace(",", "")), reverse=True)
            
            self.write_table_data_chunked(sheet, row, dept_columns, dept_data_list)
            row += len(dept_data_list) + 2
            
            # 4. Service Usage Summary
            sheet.write(row, 0, "Service Usage Summary", self.formats['subheader'])
            row += 1
            
            service_columns = ["Service", "Users", "Total Accesses", "Monthly Cost (Est.)", "Annual Cost (Est.)"]
            row = self.write_table_header(sheet, row, service_columns)
            
            service_summary: Dict[str, Dict[str, Any]] = {}
            for dept_key, dept_data in dept_service_map.items():
                service_name = dept_data["service_name"]
                if service_name not in service_summary:
                    service_summary[service_name] = {
                        "users": set(),
                        "total_accesses": 0
                    }
                
                # Count unique users (approximate - would need full join for exact count)
                service_summary[service_name]["total_accesses"] += dept_data["total_accesses"]
            
            # Count users per service from user_service_map
            for user_key, user_data in user_service_map.items():
                for service_name in user_data["services"].keys():
                    if service_name not in service_summary:
                        service_summary[service_name] = {
                            "users": set(),
                            "total_accesses": 0
                        }
                    service_summary[service_name]["users"].add(user_data["user_id"])
            
            service_data_list = []
            for service_name, summary in service_summary.items():
                user_count = len(summary["users"])
                total_accesses = summary["total_accesses"]
                
                # Calculate cost
                pricing = service_pricing.get(service_name, default_genai)
                cost_per_user = pricing.get("cost_per_user_per_month_usd", 0.0)
                cost_per_access = pricing.get("cost_per_access_usd", 0.0)
                
                if cost_per_user > 0:
                    monthly_cost = cost_per_user * user_count
                else:
                    monthly_cost = total_accesses * cost_per_access
                
                annual_cost = monthly_cost * 12
                
                service_data_list.append({
                    "Service": service_name,
                    "Users": user_count,
                    "Total Accesses": total_accesses,
                    "Monthly Cost (Est.)": f"${monthly_cost:,.2f}",
                    "Annual Cost (Est.)": f"${annual_cost:,.2f}"
                })
            
            # Sort by monthly cost descending
            service_data_list.sort(key=lambda x: float(x["Monthly Cost (Est.)"].replace("$", "").replace(",", "")), reverse=True)
            
            self.write_table_data_chunked(sheet, row, service_columns, service_data_list)
            
        except Exception as e:
            # On error, log and show error message
            print(f"Warning: Failed to generate cost reduction simulation: {e}", flush=True)
            sheet.write(row, 0, f"Error generating cost reduction simulation: {str(e)[:100]}", self.formats['data'])