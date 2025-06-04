#!/usr/bin/env python3
"""
FraudDetectX A/B Testing Tool
Compare multiple model versions and visualize results
"""
import os
import sys
import json
import mlflow
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.metrics import confusion_matrix
import psycopg2
import joblib
import argparse
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ModelComparison:
    def __init__(self, output_dir="model_comparison_results"):
        """Initialize model comparison tool"""
        self.db_params = {
            'host': os.getenv('POSTGRES_HOST', 'postgres'),
            'port': os.getenv('POSTGRES_PORT', '5432'),
            'dbname': os.getenv('POSTGRES_DB', 'fraud_detection'),
            'user': os.getenv('POSTGRES_USER', 'mluser'),
            'password': os.getenv('POSTGRES_PASSWORD', 'mlpassword')
        }
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)

    def get_available_models(self):
        """Get list of available trained models"""
        try:
            conn = psycopg2.connect(**self.db_params)
            cursor = conn.cursor()

            cursor.execute("""
                SELECT 
                    id,
                    model_name,
                    model_version,
                    accuracy,
                    precision_score,
                    recall_score,
                    f1_score,
                    auc_score,
                    training_start,
                    model_path
                FROM model_training_runs
                WHERE status = 'completed'
                ORDER BY training_start DESC
            """)

            models = cursor.fetchall()
            conn.close()

            if not models:
                logger.error("No trained models found in database")
                return []

            return [
                {
                    'id': m[0],
                    'name': m[1],
                    'version': m[2],
                    'accuracy': m[3],
                    'precision': m[4],
                    'recall': m[5],
                    'f1': m[6],
                    'auc': m[7],
                    'training_date': m[8],
                    'path': m[9]
                }
                for m in models
            ]

        except Exception as e:
            logger.error(f"Database error: {e}")
            return []

    def load_test_data(self, sample_size=10000):
        """Load test data from database"""
        try:
            conn = psycopg2.connect(**self.db_params)

            query = f"""
                WITH recent_data AS (
                    SELECT 
                        t.transaction_id,
                        t.user_id,
                        t.amount,
                        t.merchant_category,
                        pt.hour_of_day,
                        pt.day_of_week,
                        pt.is_weekend,
                        pt.is_night,
                        pt.amount_zscore,
                        pt.user_avg_amount,
                        pt.high_risk_merchant,
                        pt.amount_round,
                        t.is_fraud
                    FROM transactions t
                    JOIN processed_transactions pt ON t.transaction_id = pt.transaction_id
                    ORDER BY t.created_at DESC
                    LIMIT {sample_size * 2}
                ),
                fraud_samples AS (
                    SELECT *
                    FROM recent_data
                    WHERE is_fraud = TRUE
                    LIMIT {sample_size // 10}
                ),
                non_fraud_samples AS (
                    SELECT *
                    FROM recent_data
                    WHERE is_fraud = FALSE
                    LIMIT {sample_size - (sample_size // 10)}
                )
                SELECT * FROM fraud_samples
                UNION ALL
                SELECT * FROM non_fraud_samples
            """

            df = pd.read_sql(query, conn)
            conn.close()

            if df.empty:
                logger.error("No test data available")
                return None

            logger.info(f"Loaded {len(df)} transactions for testing")
            logger.info(f"Fraud samples: {df['is_fraud'].sum()} ({df['is_fraud'].mean()*100:.2f}%)")

            # Extract features and target
            X = df.drop(['transaction_id', 'user_id', 'is_fraud'], axis=1)
            y = df['is_fraud']

            return {
                'X': X,
                'y': y,
                'df': df
            }

        except Exception as e:
            logger.error(f"Error loading test data: {e}")
            return None

    def load_model(self, model_path):
        """Load model from file path"""
        try:
            model = joblib.load(model_path)
            return model
        except Exception as e:
            logger.error(f"Failed to load model from {model_path}: {e}")
            return None

    def evaluate_model(self, model, model_info, test_data):
        """Evaluate a single model"""
        if model is None or test_data is None:
            return None

        X, y = test_data['X'], test_data['y']

        # Make predictions
        start_time = time.time()
        y_pred = model.predict(X)
        y_prob = model.predict_proba(X)[:, 1] if hasattr(model, 'predict_proba') else None
        inference_time = time.time() - start_time

        # Calculate metrics
        accuracy = accuracy_score(y, y_pred)
        precision = precision_score(y, y_pred)
        recall = recall_score(y, y_pred)
        f1 = f1_score(y, y_pred)
        auc = roc_auc_score(y, y_prob) if y_prob is not None else None

        # Calculate confusion matrix
        cm = confusion_matrix(y, y_pred)

        # Business metrics
        avg_transaction = test_data['df']['amount'].mean()
        fraud_transactions = test_data['df'][test_data['df']['is_fraud']]['amount'].sum()

        tn, fp, fn, tp = cm.ravel()
        detection_rate = tp / (tp + fn) if (tp + fn) > 0 else 0

        # Estimated business impact
        avg_fraud_amount = test_data['df'][test_data['df']['is_fraud']]['amount'].mean()
        investigation_cost = 50  # $50 per investigation
        fraud_loss_rate = 0.7  # 70% of fraud amount is typically lost

        prevented_fraud = tp * avg_fraud_amount * fraud_loss_rate
        investigation_costs = (tp + fp) * investigation_cost
        missed_fraud_cost = fn * avg_fraud_amount * fraud_loss_rate
        net_savings = prevented_fraud - investigation_costs - missed_fraud_cost

        return {
            'model_id': model_info['id'],
            'model_name': model_info['name'],
            'model_version': model_info['version'],
            'training_date': model_info['training_date'],
            'metrics': {
                'accuracy': accuracy,
                'precision': precision,
                'recall': recall,
                'f1': f1,
                'auc': auc,
                'inference_time_ms': inference_time * 1000 / len(X)
            },
            'confusion_matrix': {
                'true_negative': int(tn),
                'false_positive': int(fp),
                'false_negative': int(fn),
                'true_positive': int(tp)
            },
            'business_impact': {
                'prevented_fraud': prevented_fraud,
                'investigation_costs': investigation_costs,
                'missed_fraud_cost': missed_fraud_cost,
                'net_savings': net_savings,
                'roi': (net_savings / investigation_costs) if investigation_costs > 0 else 0
            }
        }

    def compare_models(self, model_ids=None, count=3):
        """Compare multiple models"""
        # Get available models
        all_models = self.get_available_models()

        if not all_models:
            logger.error("No models available for comparison")
            return None

        # Filter models by ID if specified
        if model_ids:
            models_to_compare = [m for m in all_models if m['id'] in model_ids]
        else:
            # Otherwise use the most recent ones
            models_to_compare = all_models[:count]

        if not models_to_compare:
            logger.error("No matching models found for comparison")
            return None

        logger.info(f"Comparing {len(models_to_compare)} models")

        # Load test data
        test_data = self.load_test_data()

        if not test_data:
            logger.error("Cannot compare models without test data")
            return None

        # Evaluate each model
        results = []
        for model_info in models_to_compare:
            logger.info(f"Evaluating model: {model_info['name']} v{model_info['version']}")

            model = self.load_model(model_info['path'])
            if model:
                result = self.evaluate_model(model, model_info, test_data)
                if result:
                    results.append(result)

        if not results:
            logger.error("No valid evaluation results")
            return None

        # Find best model
        best_model = max(results, key=lambda x: x['metrics']['f1'])
        best_model_id = best_model['model_id']

        # Create comparison report
        comparison = {
            'timestamp': datetime.now().isoformat(),
            'test_data_size': len(test_data['X']),
            'fraud_rate': float(test_data['y'].mean()),
            'models_compared': len(results),
            'results': results,
            'best_model': {
                'id': best_model['model_id'],
                'name': best_model['model_name'],
                'version': best_model['model_version'],
                'f1_score': best_model['metrics']['f1']
            }
        }

        # Save comparison report
        report_file = os.path.join(self.output_dir, f"model_comparison_{int(time.time())}.json")
        with open(report_file, 'w') as f:
            json.dump(comparison, f, indent=2)

        # Generate visualizations
        self.generate_comparison_charts(results)

        return comparison

    def generate_comparison_charts(self, results):
        """Generate comparison charts"""
        if not results:
            return

        # Prepare data
        model_names = [f"{r['model_name']} v{r['model_version']}" for r in results]
        metrics = ['accuracy', 'precision', 'recall', 'f1', 'auc']

        # Set up the plots
        plt.figure(figsize=(15, 10))

        # 1. Performance metrics comparison
        plt.subplot(2, 2, 1)
        performance_data = []

        for metric in metrics:
            metric_values = [r['metrics'].get(metric, 0) for r in results]
            performance_data.append(metric_values)

        x = np.arange(len(model_names))
        width = 0.15

        for i, metric in enumerate(metrics):
            plt.bar(x + i*width - width*2, performance_data[i], width, label=metric.capitalize())

        plt.xlabel('Model')
        plt.ylabel('Score')
        plt.title('Performance Metrics Comparison')
        plt.xticks(x, model_names, rotation=45, ha='right')
        plt.legend()
        plt.grid(True, alpha=0.3)

        # 2. Confusion Matrix Heatmap (for best model)
        plt.subplot(2, 2, 2)
        best_model = max(results, key=lambda x: x['metrics']['f1'])
        cm = np.array([
            [best_model['confusion_matrix']['true_negative'], best_model['confusion_matrix']['false_positive']],
            [best_model['confusion_matrix']['false_negative'], best_model['confusion_matrix']['true_positive']]
        ])

        sns.heatmap(cm, annot=True, fmt='d', cmap='Blues',
                    xticklabels=['Legitimate', 'Fraud'],
                    yticklabels=['Legitimate', 'Fraud'])
        plt.title(f"Confusion Matrix: {best_model['model_name']} v{best_model['model_version']}")
        plt.ylabel('True Label')
        plt.xlabel('Predicted Label')

        # 3. Business Impact
        plt.subplot(2, 2, 3)
        impact_data = {
            'prevented_fraud': [r['business_impact']['prevented_fraud'] for r in results],
            'investigation_costs': [r['business_impact']['investigation_costs'] for r in results],
            'missed_fraud_cost': [r['business_impact']['missed_fraud_cost'] for r in results],
            'net_savings': [r['business_impact']['net_savings'] for r in results]
        }

        df_impact = pd.DataFrame(impact_data, index=model_names)
        df_impact.plot(kind='bar', ax=plt.gca())
        plt.title('Business Impact Comparison ($)')
        plt.xlabel('Model')
        plt.ylabel('Amount ($)')
        plt.xticks(rotation=45, ha='right')
        plt.grid(True, alpha=0.3)

        # 4. ROI Comparison
        plt.subplot(2, 2, 4)
        roi_data = [r['business_impact']['roi'] for r in results]
        plt.bar(model_names, roi_data, color='green')
        plt.title('Return on Investment (ROI)')
        plt.xlabel('Model')
        plt.ylabel('ROI (ratio)')
        plt.xticks(rotation=45, ha='right')
        plt.grid(True, alpha=0.3)

        plt.tight_layout()
        plt.savefig(os.path.join(self.output_dir, f"model_comparison_{int(time.time())}.png"))
        plt.close()

        # Save raw data for interactive dashboards
        comparison_df = pd.DataFrame({
            'model': model_names,
            'accuracy': [r['metrics']['accuracy'] for r in results],
            'precision': [r['metrics']['precision'] for r in results],
            'recall': [r['metrics']['recall'] for r in results],
            'f1_score': [r['metrics']['f1'] for r in results],
            'auc_roc': [r['metrics']['auc'] for r in results],
            'inference_time_ms': [r['metrics']['inference_time_ms'] for r in results],
            'true_positives': [r['confusion_matrix']['true_positive'] for r in results],
            'false_positives': [r['confusion_matrix']['false_positive'] for r in results],
            'true_negatives': [r['confusion_matrix']['true_negative'] for r in results],
            'false_negatives': [r['confusion_matrix']['false_negative'] for r in results],
            'prevented_fraud': [r['business_impact']['prevented_fraud'] for r in results],
            'investigation_costs': [r['business_impact']['investigation_costs'] for r in results],
            'missed_fraud_cost': [r['business_impact']['missed_fraud_cost'] for r in results],
            'net_savings': [r['business_impact']['net_savings'] for r in results],
            'roi': [r['business_impact']['roi'] for r in results]
        })

        comparison_df.to_csv(os.path.join(self.output_dir, f"model_comparison_{int(time.time())}.csv"), index=False)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='FraudDetectX Model Comparison Tool')
    parser.add_argument('--models', type=str, help='Comma-separated list of model IDs to compare')
    parser.add_argument('--count', type=int, default=3, help='Number of recent models to compare')
    parser.add_argument('--output', type=str, default='model_comparison_results', help='Output directory')

    args = parser.parse_args()

    comparator = ModelComparison(output_dir=args.output)

    model_ids = [int(id.strip()) for id in args.models.split(',')] if args.models else None

    results = comparator.compare_models(model_ids=model_ids, count=args.count)

    if results:
        print(f"Comparison completed. Best model: {results['best_model']['name']} v{results['best_model']['version']}")
        print(f"Results saved to: {args.output}")