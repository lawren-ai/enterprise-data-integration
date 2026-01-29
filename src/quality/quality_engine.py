"""
Data Quality Engine
Executes validation rules and tracks results
"""

import pandas as pd
from datetime import datetime
import hashlib
import time
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).parent.parent))

from utils.config_loader import get_config
from utils.logger import get_logger
from utils.db_manager import get_db_manager
from quality.validation_rules import ValidationRules

logger = get_logger(__name__)


class QualityEngine:
    """Executes data quality rules and manages results"""
    
    def __init__(self):
        self.config = get_config()
        self.db = get_db_manager()
        self.rules = ValidationRules.get_all_rules()
        
        logger.info("QualityEngine initialized")
    
    def setup_rules(self):
        """Load rules into database"""
        logger.info("Setting up quality rules in database...")
        
        # Get category IDs
        categories = self.db.read_query("SELECT category_id, category_name FROM dq_rule_categories")
        category_map = dict(zip(categories['category_name'], categories['category_id']))
        
        rules_loaded = 0
        
        for rule in self.rules:
            # Check if rule exists
            existing = self.db.read_query("""
                SELECT rule_id FROM dq_rules WHERE rule_name = :name
            """, {'name': rule['rule_name']})
            
            if len(existing) > 0:
                # Update existing rule
                self.db.execute_sql("""
                    UPDATE dq_rules
                    SET rule_description = :desc,
                        category_id = :cat_id,
                        target_table = :table,
                        target_column = :column,
                        rule_type = :type,
                        rule_sql = :sql,
                        severity = :severity,
                        failure_threshold = :threshold,
                        updated_date = CURRENT_TIMESTAMP
                    WHERE rule_name = :name
                """, {
                    'desc': rule['rule_description'],
                    'cat_id': category_map[rule['category']],
                    'table': rule['target_table'],
                    'column': rule.get('target_column'),
                    'type': rule['rule_type'],
                    'sql': rule['rule_sql'],
                    'severity': rule['severity'],
                    'threshold': rule['failure_threshold'],
                    'name': rule['rule_name']
                })
            else:
                # Insert new rule
                self.db.execute_sql("""
                    INSERT INTO dq_rules (
                        rule_name, rule_description, category_id,
                        target_table, target_column, rule_type, rule_sql,
                        severity, failure_threshold, is_active
                    ) VALUES (
                        :name, :desc, :cat_id, :table, :column, :type, :sql,
                        :severity, :threshold, TRUE
                    )
                """, {
                    'name': rule['rule_name'],
                    'desc': rule['rule_description'],
                    'cat_id': category_map[rule['category']],
                    'table': rule['target_table'],
                    'column': rule.get('target_column'),
                    'type': rule['rule_type'],
                    'sql': rule['rule_sql'],
                    'severity': rule['severity'],
                    'threshold': rule['failure_threshold']
                })
            
            rules_loaded += 1
        
        logger.info(f"✓ Loaded {rules_loaded} quality rules")
    
    def execute_rule(self, rule: dict) -> dict:
        """
        Execute a single quality rule
        
        Returns:
            dict with execution results
        """
        rule_name = rule['rule_name']
        logger.info(f"Executing rule: {rule_name}")
        
        start_time = time.time()
        
        try:
            # Get total records to check
            total_query = f"SELECT COUNT(*) as cnt FROM {rule['target_table']}"
            total_result = self.db.read_query(total_query)
            total_records = total_result.iloc[0]['cnt']
            
            # Execute validation SQL
            violations_df = self.db.read_query(rule['rule_sql'])
            failed_records = len(violations_df)
            passed_records = total_records - failed_records
            
            # Calculate metrics
            failure_percentage = (failed_records / total_records * 100) if total_records > 0 else 0
            
            # Determine status
            if failed_records == 0:
                test_status = 'PASSED'
                test_message = 'All records passed validation'
            elif failure_percentage <= rule['failure_threshold']:
                test_status = 'WARNING'
                test_message = f"{failed_records} violations found but within threshold ({rule['failure_threshold']}%)"
            else:
                test_status = 'FAILED'
                test_message = f"{failed_records} violations exceed threshold ({rule['failure_threshold']}%)"
            
            execution_time = int((time.time() - start_time) * 1000)
            
            result = {
                'rule_name': rule_name,
                'total_records': total_records,
                'failed_records': failed_records,
                'passed_records': passed_records,
                'failure_percentage': round(failure_percentage, 2),
                'test_status': test_status,
                'test_message': test_message,
                'execution_time_ms': execution_time,
                'violations': violations_df
            }
            
            logger.info(f"  Status: {test_status}, Failures: {failed_records}/{total_records} ({failure_percentage:.2f}%)")
            
            return result
            
        except Exception as e:
            logger.error(f"Rule execution failed: {e}")
            return {
                'rule_name': rule_name,
                'total_records': 0,
                'failed_records': 0,
                'passed_records': 0,
                'failure_percentage': 0,
                'test_status': 'ERROR',
                'test_message': str(e),
                'execution_time_ms': 0,
                'violations': pd.DataFrame()
            }
    
    def save_result(self, rule_id: int, result: dict):
        """Save execution result to database"""
        self.db.execute_sql("""
            INSERT INTO dq_test_results (
                rule_id, execution_duration_ms, total_records_checked,
                failed_records, passed_records, failure_percentage,
                test_status, test_message
            ) VALUES (
                :rule_id, :duration, :total, :failed, :passed, 
                :pct, :status, :message
            )
        """, {
            'rule_id': int(rule_id),
            'duration': int(result['execution_time_ms']),
            'total': int(result['total_records']),
            'failed': int(result['failed_records']),
            'passed': int(result['passed_records']),
            'pct': float(result['failure_percentage']),
            'status': str(result['test_status']),
            'message': str(result['test_message'])
        })
        
        # Get the result_id for exceptions
        result_id = self.db.read_query("""
            SELECT result_id FROM dq_test_results 
            WHERE rule_id = :rule_id 
            ORDER BY execution_date DESC 
            LIMIT 1
        """, {'rule_id': rule_id}).iloc[0]['result_id']
        
        # Save exceptions
        if len(result['violations']) > 0:
            self.save_exceptions(result_id, rule_id, result['violations'])
    
    def save_exceptions(self, result_id: int, rule_id: int, violations: pd.DataFrame):
        """Save detailed exceptions (limited to first 1000)"""
        # Get rule details
        rule_info = self.db.read_query("""
            SELECT target_table FROM dq_rules WHERE rule_id = :id
        """, {'id': rule_id}).iloc[0]
        
        # Limit exceptions to prevent database overload
        max_exceptions = 1000
        violations_to_save = violations.head(max_exceptions)
        
        if len(violations) > max_exceptions:
            logger.warning(f"Limiting exceptions to {max_exceptions} of {len(violations)} total violations")
        
        for _, row in violations_to_save.iterrows():
            # Create hash for duplicate detection
            exception_str = f"{rule_id}_{row['record_identifier']}_{row.get('column_name', '')}_{row.get('failed_value', '')}"
            exception_hash = hashlib.md5(exception_str.encode()).hexdigest()
            
            self.db.execute_sql("""
                INSERT INTO dq_exceptions (
                    result_id, rule_id, table_name, record_identifier,
                    column_name, failed_value, expected_value, exception_hash
                ) VALUES (
                    :result_id, :rule_id, :table, :identifier,
                    :column, :failed, :expected, :hash
                )
            """, {
                'result_id': int(result_id),
                'rule_id': int(rule_id),
                'table': str(rule_info['target_table']),
                'identifier': str(row['record_identifier']),
                'column': str(row.get('column_name', '')),
                'failed': str(row.get('failed_value', ''))[:1000],  # Truncate long values
                'expected': str(row.get('expected_value', ''))[:1000],
                'hash': str(exception_hash)
            })
    
    def run_all_rules(self) -> dict:
        """Execute all active rules"""
        logger.info("=" * 80)
        logger.info("STARTING DATA QUALITY VALIDATION")
        logger.info("=" * 80)
        
        # Get active rules from database
        active_rules = self.db.read_query("""
            SELECT rule_id, rule_name FROM dq_rules WHERE is_active = TRUE
        """)
        
        results_summary = {
            'total_rules': len(active_rules),
            'passed': 0,
            'failed': 0,
            'warning': 0,
            'error': 0
        }
        
        for _, db_rule in active_rules.iterrows():
            # Find matching rule definition
            rule_def = next((r for r in self.rules if r['rule_name'] == db_rule['rule_name']), None)
            
            if rule_def:
                result = self.execute_rule(rule_def)
                self.save_result(db_rule['rule_id'], result)
                
                # Update summary
                if result['test_status'] == 'PASSED':
                    results_summary['passed'] += 1
                elif result['test_status'] == 'FAILED':
                    results_summary['failed'] += 1
                elif result['test_status'] == 'WARNING':
                    results_summary['warning'] += 1
                else:
                    results_summary['error'] += 1
        
        logger.info("=" * 80)
        logger.info("DATA QUALITY VALIDATION COMPLETED")
        logger.info(f"Total Rules: {results_summary['total_rules']}")
        logger.info(f"Passed: {results_summary['passed']}")
        logger.info(f"Warning: {results_summary['warning']}")
        logger.info(f"Failed: {results_summary['failed']}")
        logger.info(f"Error: {results_summary['error']}")
        logger.info("=" * 80)
        
        return results_summary
    
    def generate_scorecard(self, report_period='DAILY'):
        """Generate quality scorecard"""
        logger.info(f"Generating {report_period} quality scorecard...")
        
        # Get today's results
        today_results = self.db.read_query("""
            SELECT 
                r.category_id,
                c.category_name,
                tr.test_status,
                COUNT(*) as rule_count,
                SUM(tr.total_records_checked) as total_records,
                SUM(tr.failed_records) as failed_records
            FROM dq_test_results tr
            JOIN dq_rules r ON tr.rule_id = r.rule_id
            JOIN dq_rule_categories c ON r.category_id = c.category_id
            WHERE tr.execution_date::date = CURRENT_DATE
            GROUP BY r.category_id, c.category_name, tr.test_status
        """)
        
        if len(today_results) == 0:
            logger.warning("No results found for today")
            return
        
        # Calculate scores by category
        category_scores = {}
        for category in ['Completeness', 'Accuracy', 'Consistency', 'Validity', 'Uniqueness', 'Timeliness', 'Integrity']:
            cat_data = today_results[today_results['category_name'] == category]
            if len(cat_data) > 0:
                total_records = cat_data['total_records'].sum()
                failed_records = cat_data['failed_records'].sum()
                score = ((total_records - failed_records) / total_records * 100) if total_records > 0 else 100
                category_scores[category.lower() + '_score'] = round(score, 2)
            else:
                category_scores[category.lower() + '_score'] = 100.0
        
        # Overall metrics
        rules_by_status = today_results.groupby('test_status')['rule_count'].sum().to_dict()
        
        overall_score = sum(category_scores.values()) / len(category_scores)
        
        # Insert scorecard
        self.db.execute_sql("""
            INSERT INTO dq_scorecards (
                report_date, report_period,
                total_rules_executed, rules_passed, rules_failed, rules_warning,
                overall_quality_score, completeness_score, accuracy_score,
                consistency_score, validity_score, uniqueness_score,
                timeliness_score, integrity_score,
                total_records_checked, total_failed_records
            ) VALUES (
                CURRENT_DATE, :period,
                :total_rules, :passed, :failed, :warning,
                :overall, :completeness, :accuracy,
                :consistency, :validity, :uniqueness,
                :timeliness, :integrity,
                :total_records, :failed_records
            )
            ON CONFLICT (report_date, report_period) 
            DO UPDATE SET
                total_rules_executed = EXCLUDED.total_rules_executed,
                rules_passed = EXCLUDED.rules_passed,
                rules_failed = EXCLUDED.rules_failed,
                rules_warning = EXCLUDED.rules_warning,
                overall_quality_score = EXCLUDED.overall_quality_score,
                completeness_score = EXCLUDED.completeness_score,
                accuracy_score = EXCLUDED.accuracy_score,
                consistency_score = EXCLUDED.consistency_score,
                validity_score = EXCLUDED.validity_score,
                uniqueness_score = EXCLUDED.uniqueness_score,
                timeliness_score = EXCLUDED.timeliness_score,
                integrity_score = EXCLUDED.integrity_score,
                total_records_checked = EXCLUDED.total_records_checked,
                total_failed_records = EXCLUDED.total_failed_records
        """, {
            'period': report_period,
            'total_rules': sum(rules_by_status.values()),
            'passed': rules_by_status.get('PASSED', 0),
            'failed': rules_by_status.get('FAILED', 0),
            'warning': rules_by_status.get('WARNING', 0),
            'overall': round(overall_score, 2),
            'completeness': category_scores.get('completeness_score', 100),
            'accuracy': category_scores.get('accuracy_score', 100),
            'consistency': category_scores.get('consistency_score', 100),
            'validity': category_scores.get('validity_score', 100),
            'uniqueness': category_scores.get('uniqueness_score', 100),
            'timeliness': category_scores.get('timeliness_score', 100),
            'integrity': category_scores.get('integrity_score', 100),
            'total_records': today_results['total_records'].sum(),
            'failed_records': today_results['failed_records'].sum()
        })
        
        logger.info(f"✓ Quality scorecard generated - Overall score: {overall_score:.2f}%")


def main():
    """Main execution"""
    engine = QualityEngine()
    
    # Setup rules
    engine.setup_rules()
    
    # Run validation
    results = engine.run_all_rules()
    
    # Generate scorecard
    engine.generate_scorecard()


if __name__ == "__main__":
    main()