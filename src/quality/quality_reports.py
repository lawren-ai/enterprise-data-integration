"""
Data Quality Reports Generator
Generates HTML reports and visualizations for quality metrics
"""

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')  # Non-interactive backend
import seaborn as sns
from datetime import datetime, timedelta
from pathlib import Path
import base64
from io import BytesIO
import sys

sys.path.append(str(Path(__file__).parent.parent))

from utils.config_loader import get_config
from utils.logger import get_logger
from utils.db_manager import get_db_manager

logger = get_logger(__name__)

# Set style for professional-looking charts
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (12, 6)
plt.rcParams['font.size'] = 10


class QualityReportGenerator:
    """Generate comprehensive data quality reports"""
    
    def __init__(self, output_dir: str = "reports"):
        self.config = get_config()
        self.db = get_db_manager()
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"QualityReportGenerator initialized - Output: {self.output_dir}")
    
    def _fig_to_base64(self, fig) -> str:
        """Convert matplotlib figure to base64 string for embedding in HTML"""
        buffer = BytesIO()
        fig.savefig(buffer, format='png', bbox_inches='tight', dpi=100)
        buffer.seek(0)
        image_base64 = base64.b64encode(buffer.read()).decode()
        plt.close(fig)
        return f"data:image/png;base64,{image_base64}"
    
    def _get_latest_scorecard(self) -> pd.DataFrame:
        """Get the most recent quality scorecard"""
        return self.db.read_query("""
            SELECT *
            FROM dq_scorecards
            ORDER BY report_date DESC
            LIMIT 1
        """)
    
    def _get_scorecard_trends(self, days: int = 30) -> pd.DataFrame:
        """Get quality scorecard trends over time"""
        return self.db.read_query(f"""
            SELECT 
                report_date,
                overall_quality_score,
                completeness_score,
                accuracy_score,
                consistency_score,
                validity_score,
                uniqueness_score,
                timeliness_score,
                integrity_score,
                rules_passed,
                rules_warning,
                rules_failed
            FROM dq_scorecards
            WHERE report_date >= CURRENT_DATE - INTERVAL '{days} days'
            ORDER BY report_date
        """)
    
    def _get_recent_test_results(self, days: int = 1) -> pd.DataFrame:
        """Get recent test results with rule details"""
        return self.db.read_query(f"""
            SELECT 
                r.rule_name,
                r.rule_description,
                c.category_name,
                r.target_table,
                r.severity,
                tr.test_status,
                tr.total_records_checked,
                tr.failed_records,
                tr.passed_records,
                tr.failure_percentage,
                tr.test_message,
                tr.execution_date,
                tr.execution_duration_ms
            FROM dq_test_results tr
            JOIN dq_rules r ON tr.rule_id = r.rule_id
            JOIN dq_rule_categories c ON r.category_id = c.category_id
            WHERE tr.execution_date >= CURRENT_DATE - INTERVAL '{days} days'
            ORDER BY tr.execution_date DESC, c.category_name, r.rule_name
        """)
    
    def _get_top_exceptions(self, limit: int = 100) -> pd.DataFrame:
        """Get most recent exceptions"""
        return self.db.read_query(f"""
            SELECT 
                r.rule_name,
                c.category_name,
                e.table_name,
                e.record_identifier,
                e.column_name,
                e.failed_value,
                e.expected_value,
                e.detected_date
            FROM dq_exceptions e
            JOIN dq_rules r ON e.rule_id = r.rule_id
            JOIN dq_rule_categories c ON r.category_id = c.category_id
            WHERE e.detected_date >= CURRENT_DATE - INTERVAL '7 days'
            ORDER BY e.detected_date DESC
            LIMIT {limit}
        """)
    
    def _create_scorecard_chart(self, scorecard: pd.Series) -> str:
        """Create overall quality scorecard chart"""
        fig, ax = plt.subplots(figsize=(10, 6))
        
        dimensions = ['Completeness', 'Accuracy', 'Consistency', 'Validity', 
                     'Uniqueness', 'Timeliness', 'Integrity']
        scores = [
            scorecard['completeness_score'],
            scorecard['accuracy_score'],
            scorecard['consistency_score'],
            scorecard['validity_score'],
            scorecard['uniqueness_score'],
            scorecard['timeliness_score'],
            scorecard['integrity_score']
        ]
        
        # Create color mapping based on score
        colors = ['#2ecc71' if s >= 95 else '#f39c12' if s >= 80 else '#e74c3c' for s in scores]
        
        bars = ax.barh(dimensions, scores, color=colors)
        ax.set_xlabel('Quality Score (%)', fontsize=12, fontweight='bold')
        ax.set_title('Data Quality Dimensions', fontsize=14, fontweight='bold', pad=20)
        ax.set_xlim(0, 100)
        ax.axvline(x=95, color='green', linestyle='--', alpha=0.3, label='Excellent (95%+)')
        ax.axvline(x=80, color='orange', linestyle='--', alpha=0.3, label='Good (80%+)')
        
        # Add value labels on bars
        for bar, score in zip(bars, scores):
            width = bar.get_width()
            ax.text(width + 1, bar.get_y() + bar.get_height()/2, 
                   f'{score:.1f}%', 
                   ha='left', va='center', fontweight='bold')
        
        ax.legend(loc='lower right')
        ax.grid(axis='x', alpha=0.3)
        
        return self._fig_to_base64(fig)
    
    def _create_trend_chart(self, trends: pd.DataFrame) -> str:
        """Create quality score trend chart"""
        if len(trends) == 0:
            # Return placeholder if no data
            fig, ax = plt.subplots(figsize=(12, 6))
            ax.text(0.5, 0.5, 'No trend data available', 
                   ha='center', va='center', fontsize=14)
            ax.set_xlim(0, 1)
            ax.set_ylim(0, 1)
            return self._fig_to_base64(fig)
        
        fig, ax = plt.subplots(figsize=(12, 6))
        
        trends['report_date'] = pd.to_datetime(trends['report_date'])
        
        ax.plot(trends['report_date'], trends['overall_quality_score'], 
               marker='o', linewidth=2, markersize=8, label='Overall Score', color='#3498db')
        ax.plot(trends['report_date'], trends['completeness_score'], 
               marker='s', linewidth=1, markersize=6, label='Completeness', alpha=0.7)
        ax.plot(trends['report_date'], trends['accuracy_score'], 
               marker='^', linewidth=1, markersize=6, label='Accuracy', alpha=0.7)
        ax.plot(trends['report_date'], trends['validity_score'], 
               marker='d', linewidth=1, markersize=6, label='Validity', alpha=0.7)
        
        ax.set_xlabel('Date', fontsize=12, fontweight='bold')
        ax.set_ylabel('Quality Score (%)', fontsize=12, fontweight='bold')
        ax.set_title('Quality Score Trends', fontsize=14, fontweight='bold', pad=20)
        ax.set_ylim(0, 105)
        ax.axhline(y=95, color='green', linestyle='--', alpha=0.3)
        ax.axhline(y=80, color='orange', linestyle='--', alpha=0.3)
        ax.legend(loc='best', framealpha=0.9)
        ax.grid(True, alpha=0.3)
        
        plt.xticks(rotation=45)
        plt.tight_layout()
        
        return self._fig_to_base64(fig)
    
    def _create_rules_status_chart(self, scorecard: pd.Series) -> str:
        """Create pie chart for rules status distribution"""
        fig, ax = plt.subplots(figsize=(8, 8))
        
        labels = ['Passed', 'Warning', 'Failed']
        sizes = [
            scorecard['rules_passed'],
            scorecard['rules_warning'],
            scorecard['rules_failed']
        ]
        colors = ['#2ecc71', '#f39c12', '#e74c3c']
        explode = (0.05, 0.05, 0.1)
        
        wedges, texts, autotexts = ax.pie(sizes, explode=explode, labels=labels, colors=colors,
                                           autopct='%1.1f%%', startangle=90, textprops={'fontsize': 12})
        
        for autotext in autotexts:
            autotext.set_color('white')
            autotext.set_fontweight('bold')
        
        ax.set_title('Rules Status Distribution', fontsize=14, fontweight='bold', pad=20)
        
        return self._fig_to_base64(fig)
    
    def generate_html_report(self, report_name: str = "quality_dashboard") -> Path:
        """Generate comprehensive HTML quality report"""
        logger.info("Generating HTML quality report...")
        
        # Fetch data
        scorecard_df = self._get_latest_scorecard()
        if len(scorecard_df) == 0:
            logger.error("No scorecard data available. Please run quality checks first.")
            return None
        
        scorecard = scorecard_df.iloc[0]
        trends = self._get_scorecard_trends(days=30)
        test_results = self._get_recent_test_results(days=1)
        exceptions = self._get_top_exceptions(limit=100)
        
        # Generate charts
        scorecard_chart = self._create_scorecard_chart(scorecard)
        trend_chart = self._create_trend_chart(trends)
        status_chart = self._create_rules_status_chart(scorecard)
        
        # Generate HTML
        html = self._generate_html_content(
            scorecard, test_results, exceptions,
            scorecard_chart, trend_chart, status_chart
        )
        
        # Save report
        report_file = self.output_dir / f"{report_name}_{datetime.now():%Y%m%d_%H%M%S}.html"
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(html)
        
        logger.info(f"✓ Quality report generated: {report_file}")
        return report_file
    
    def _generate_html_content(self, scorecard, test_results, exceptions,
                               scorecard_chart, trend_chart, status_chart) -> str:
        """Generate the complete HTML content"""
        
        # Status indicator color
        overall_score = scorecard['overall_quality_score']
        if overall_score >= 95:
            status_color = '#2ecc71'
            status_text = 'EXCELLENT'
        elif overall_score >= 80:
            status_color = '#f39c12'
            status_text = 'GOOD'
        elif overall_score >= 60:
            status_color = '#e67e22'
            status_text = 'FAIR'
        else:
            status_color = '#e74c3c'
            status_text = 'NEEDS ATTENTION'
        
        html = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Data Quality Dashboard - {datetime.now():%Y-%m-%d}</title>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            padding: 20px;
            color: #333;
        }}
        
        .container {{
            max-width: 1400px;
            margin: 0 auto;
        }}
        
        .header {{
            background: white;
            padding: 30px;
            border-radius: 10px;
            margin-bottom: 20px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        }}
        
        .header h1 {{
            color: #2c3e50;
            margin-bottom: 10px;
        }}
        
        .header .subtitle {{
            color: #7f8c8d;
            font-size: 14px;
        }}
        
        .scorecard {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin-bottom: 20px;
        }}
        
        .metric-card {{
            background: white;
            padding: 25px;
            border-radius: 10px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
            text-align: center;
        }}
        
        .metric-card .label {{
            color: #7f8c8d;
            font-size: 14px;
            text-transform: uppercase;
            letter-spacing: 1px;
            margin-bottom: 10px;
        }}
        
        .metric-card .value {{
            font-size: 36px;
            font-weight: bold;
            margin-bottom: 10px;
        }}
        
        .metric-card .subtext {{
            color: #95a5a6;
            font-size: 12px;
        }}
        
        .status-excellent {{ color: #2ecc71; }}
        .status-good {{ color: #f39c12; }}
        .status-warning {{ color: #e67e22; }}
        .status-critical {{ color: #e74c3c; }}
        
        .chart-section {{
            background: white;
            padding: 30px;
            border-radius: 10px;
            margin-bottom: 20px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        }}
        
        .chart-section h2 {{
            color: #2c3e50;
            margin-bottom: 20px;
            padding-bottom: 10px;
            border-bottom: 3px solid #3498db;
        }}
        
        .chart-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(500px, 1fr));
            gap: 30px;
            margin-bottom: 20px;
        }}
        
        .chart-container {{
            text-align: center;
        }}
        
        .chart-container img {{
            max-width: 100%;
            height: auto;
            border-radius: 5px;
        }}
        
        table {{
            width: 100%;
            border-collapse: collapse;
            background: white;
            margin-top: 10px;
        }}
        
        th, td {{
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid #ecf0f1;
        }}
        
        th {{
            background: #34495e;
            color: white;
            font-weight: 600;
            text-transform: uppercase;
            font-size: 12px;
            letter-spacing: 0.5px;
        }}
        
        tr:hover {{
            background: #f8f9fa;
        }}
        
        .badge {{
            display: inline-block;
            padding: 4px 12px;
            border-radius: 20px;
            font-size: 11px;
            font-weight: bold;
            text-transform: uppercase;
        }}
        
        .badge-passed {{
            background: #d4edda;
            color: #155724;
        }}
        
        .badge-warning {{
            background: #fff3cd;
            color: #856404;
        }}
        
        .badge-failed {{
            background: #f8d7da;
            color: #721c24;
        }}
        
        .badge-critical {{
            background: #721c24;
            color: white;
        }}
        
        .badge-info {{
            background: #d1ecf1;
            color: #0c5460;
        }}
        
        .footer {{
            text-align: center;
            color: white;
            margin-top: 30px;
            padding: 20px;
            opacity: 0.9;
        }}
        
        .severity-CRITICAL {{ color: #c0392b; font-weight: bold; }}
        .severity-WARNING {{ color: #e67e22; font-weight: bold; }}
        .severity-INFO {{ color: #3498db; font-weight: bold; }}
        
        @media print {{
            body {{ background: white; }}
            .chart-section {{ page-break-inside: avoid; }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <!-- Header -->
        <div class="header">
            <h1>🎯 Data Quality Dashboard</h1>
            <p class="subtitle">Generated on {datetime.now():%B %d, %Y at %I:%M %p} | Enterprise Data Warehouse</p>
        </div>
        
        <!-- Key Metrics -->
        <div class="scorecard">
            <div class="metric-card">
                <div class="label">Overall Quality Score</div>
                <div class="value" style="color: {status_color};">{overall_score:.1f}%</div>
                <div class="subtext">{status_text}</div>
            </div>
            
            <div class="metric-card">
                <div class="label">Rules Executed</div>
                <div class="value" style="color: #3498db;">{scorecard['total_rules_executed']}</div>
                <div class="subtext">Total validation rules</div>
            </div>
            
            <div class="metric-card">
                <div class="label">Passed</div>
                <div class="value status-excellent">{scorecard['rules_passed']}</div>
                <div class="subtext">{scorecard['rules_passed'] / scorecard['total_rules_executed'] * 100:.0f}% success rate</div>
            </div>
            
            <div class="metric-card">
                <div class="label">Failed</div>
                <div class="value status-critical">{scorecard['rules_failed']}</div>
                <div class="subtext">Requires attention</div>
            </div>
            
            <div class="metric-card">
                <div class="label">Records Checked</div>
                <div class="value" style="color: #9b59b6;">{scorecard['total_records_checked']:,}</div>
                <div class="subtext">Across all rules</div>
            </div>
            
            <div class="metric-card">
                <div class="label">Issues Found</div>
                <div class="value status-warning">{scorecard['total_failed_records']:,}</div>
                <div class="subtext">{scorecard['total_failed_records'] / scorecard['total_records_checked'] * 100:.2f}% failure rate</div>
            </div>
        </div>
        
        <!-- Charts Grid -->
        <div class="chart-grid">
            <div class="chart-section">
                <h2>📊 Quality Dimensions</h2>
                <div class="chart-container">
                    <img src="{scorecard_chart}" alt="Quality Dimensions">
                </div>
            </div>
            
            <div class="chart-section">
                <h2>📈 Rules Status</h2>
                <div class="chart-container">
                    <img src="{status_chart}" alt="Rules Status">
                </div>
            </div>
        </div>
        
        <!-- Trends -->
        <div class="chart-section">
            <h2>📉 Quality Trends (30 Days)</h2>
            <div class="chart-container">
                <img src="{trend_chart}" alt="Quality Trends">
            </div>
        </div>
        
        <!-- Test Results Detail -->
        <div class="chart-section">
            <h2>🔍 Recent Test Results</h2>
            <table>
                <thead>
                    <tr>
                        <th>Rule Name</th>
                        <th>Category</th>
                        <th>Target Table</th>
                        <th>Severity</th>
                        <th>Status</th>
                        <th>Records Checked</th>
                        <th>Failed</th>
                        <th>Failure %</th>
                    </tr>
                </thead>
                <tbody>
"""
        
        # Add test results rows
        for _, result in test_results.iterrows():
            status_badge = f"badge-{result['test_status'].lower()}"
            severity_class = f"severity-{result['severity']}"
            
            html += f"""
                    <tr>
                        <td title="{result['rule_description']}">{result['rule_name']}</td>
                        <td>{result['category_name']}</td>
                        <td><code>{result['target_table']}</code></td>
                        <td class="{severity_class}">{result['severity']}</td>
                        <td><span class="badge {status_badge}">{result['test_status']}</span></td>
                        <td>{result['total_records_checked']:,}</td>
                        <td>{result['failed_records']:,}</td>
                        <td>{result['failure_percentage']:.2f}%</td>
                    </tr>
"""
        
        html += """
                </tbody>
            </table>
        </div>
"""
        
        # Add exceptions section if any exist
        if len(exceptions) > 0:
            html += """
        <!-- Top Exceptions -->
        <div class="chart-section">
            <h2>⚠️ Recent Exceptions (Top 100)</h2>
            <table>
                <thead>
                    <tr>
                        <th>Rule</th>
                        <th>Category</th>
                        <th>Table</th>
                        <th>Record ID</th>
                        <th>Column</th>
                        <th>Failed Value</th>
                        <th>Expected</th>
                        <th>Date</th>
                    </tr>
                </thead>
                <tbody>
"""
            
            for _, exc in exceptions.head(100).iterrows():
                failed_val = str(exc['failed_value'])[:50] if pd.notna(exc['failed_value']) else 'NULL'
                expected_val = str(exc['expected_value'])[:50] if pd.notna(exc['expected_value']) else 'N/A'
                
                html += f"""
                    <tr>
                        <td>{exc['rule_name']}</td>
                        <td><span class="badge badge-info">{exc['category_name']}</span></td>
                        <td><code>{exc['table_name']}</code></td>
                        <td>{exc['record_identifier']}</td>
                        <td>{exc['column_name'] if pd.notna(exc['column_name']) else '-'}</td>
                        <td><code>{failed_val}</code></td>
                        <td><code>{expected_val}</code></td>
                        <td>{exc['detected_date']:%Y-%m-%d %H:%M}</td>
                    </tr>
"""
            
            html += """
                </tbody>
            </table>
        </div>
"""
        
        html += f"""
        <!-- Footer -->
        <div class="footer">
            <p><strong>Enterprise Data Integration Platform</strong></p>
            <p>Data Quality Framework v1.0</p>
            <p style="margin-top: 10px; font-size: 12px;">
                Report Date: {scorecard['report_date']:%Y-%m-%d} | 
                Total Rules: {scorecard['total_rules_executed']} | 
                Overall Score: {overall_score:.1f}%
            </p>
        </div>
    </div>
</body>
</html>
"""
        
        return html
    
    def generate_executive_summary(self) -> Path:
        """Generate executive summary report"""
        logger.info("Generating executive summary...")
        
        scorecard_df = self._get_latest_scorecard()
        if len(scorecard_df) == 0:
            logger.error("No scorecard data available")
            return None
        
        scorecard = scorecard_df.iloc[0]
        
        summary_file = self.output_dir / f"executive_summary_{datetime.now():%Y%m%d}.txt"
        
        with open(summary_file, 'w') as f:
            f.write("=" * 80 + "\n")
            f.write("DATA QUALITY EXECUTIVE SUMMARY\n")
            f.write(f"Report Date: {datetime.now():%B %d, %Y}\n")
            f.write("=" * 80 + "\n\n")
            
            f.write(f"OVERALL QUALITY SCORE: {scorecard['overall_quality_score']:.1f}%\n\n")
            
            f.write("KEY METRICS:\n")
            f.write(f"  • Total Rules Executed: {scorecard['total_rules_executed']}\n")
            f.write(f"  • Rules Passed: {scorecard['rules_passed']} ({scorecard['rules_passed']/scorecard['total_rules_executed']*100:.0f}%)\n")
            f.write(f"  • Rules Failed: {scorecard['rules_failed']}\n")
            f.write(f"  • Rules with Warnings: {scorecard['rules_warning']}\n")
            f.write(f"  • Records Validated: {scorecard['total_records_checked']:,}\n")
            f.write(f"  • Issues Identified: {scorecard['total_failed_records']:,}\n\n")
            
            f.write("QUALITY DIMENSIONS:\n")
            f.write(f"  • Completeness: {scorecard['completeness_score']:.1f}%\n")
            f.write(f"  • Accuracy: {scorecard['accuracy_score']:.1f}%\n")
            f.write(f"  • Consistency: {scorecard['consistency_score']:.1f}%\n")
            f.write(f"  • Validity: {scorecard['validity_score']:.1f}%\n")
            f.write(f"  • Uniqueness: {scorecard['uniqueness_score']:.1f}%\n")
            f.write(f"  • Timeliness: {scorecard['timeliness_score']:.1f}%\n")
            f.write(f"  • Integrity: {scorecard['integrity_score']:.1f}%\n\n")
            
            f.write("RECOMMENDATIONS:\n")
            if scorecard['rules_failed'] > 0:
                f.write("  • IMMEDIATE ACTION: Review and remediate failed validation rules\n")
            if scorecard['completeness_score'] < 95:
                f.write("  • Address data completeness gaps in source systems\n")
            if scorecard['overall_quality_score'] >= 95:
                f.write("  • Data quality is excellent - maintain current processes\n")
            elif scorecard['overall_quality_score'] >= 80:
                f.write("  • Data quality is good - minor improvements needed\n")
            else:
                f.write("  • Data quality needs improvement - implement corrective actions\n")
            
            f.write("\n" + "=" * 80 + "\n")
        
        logger.info(f"✓ Executive summary generated: {summary_file}")
        return summary_file


def main():
    """Main execution"""
    generator = QualityReportGenerator()
    
    # Generate HTML dashboard
    report_file = generator.generate_html_report()
    if report_file:
        logger.info(f"✓ HTML report: {report_file}")
    
    # Generate executive summary
    summary_file = generator.generate_executive_summary()
    if summary_file:
        logger.info(f"✓ Executive summary: {summary_file}")
    
    logger.info("✓ All reports generated successfully")


if __name__ == "__main__":
    main()
