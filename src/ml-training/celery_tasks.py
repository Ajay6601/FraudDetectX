"""
Celery Tasks for ML Training Pipeline with XGBoost + SMOTE + MLflow
Complete production-ready ML training system
"""

import os
import json
import pickle
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split, RandomizedSearchCV, StratifiedKFold
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.metrics import (
    accuracy_score, precision_score, recall_score, f1_score,
    roc_auc_score, average_precision_score, classification_report,
    confusion_matrix, roc_curve, precision_recall_curve
)
from sklearn.pipeline import Pipeline
from imblearn.over_sampling import SMOTE
from imblearn.pipeline import Pipeline as ImbPipeline
import xgboost as xgb
import joblib
import mlflow
import mlflow.xgboost
import mlflow.sklearn
from celery import Celery, Task
from celery.schedules import crontab
import psycopg2
import redis
from loguru import logger
import warnings
warnings.filterwarnings("ignore", message="use_label_encoder")
warnings.filterwarnings("ignore", message="n_jobs.*deprecated")
warnings.filterwarnings("ignore", module="xgboost")
warnings.filterwarnings("ignore", module="imblearn")
warnings.filterwarnings("ignore", module="joblib")
warnings.filterwarnings("ignore", message="broker_connection_retry")

# Configure logging
logger.add("/app/logs/ml_training.log", rotation="100 MB", level="INFO")

# Initialize Celery
celery_app = Celery('fraud_detection_ml')
celery_app.config_from_object('celery_config')

# Initialize Redis connection
redis_client = redis.from_url(os.getenv('CELERY_BROKER_URL', 'redis://redis:6379/0'))

class MLTrainingTask(Task):
    """Base task class with database connection"""
    _db_connection = None

    @property
    def db_connection(self):
        if self._db_connection is None:
            self._db_connection = psycopg2.connect(
                host=os.getenv('POSTGRES_HOST', 'postgres'),
                port=os.getenv('POSTGRES_PORT', '5432'),
                database=os.getenv('POSTGRES_DB', 'fraud_detection'),
                user=os.getenv('POSTGRES_USER', 'mluser'),
                password=os.getenv('POSTGRES_PASSWORD', 'mlpassword')
            )
        return self._db_connection

class FraudDetectionMLPipeline:
    """Complete ML Pipeline for Fraud Detection"""

    def __init__(self):
        self.model = None
        self.feature_columns = None
        self.scalers = {}
        self.encoders = {}
        self.model_metrics = {}

        # Configure MLflow
        mlflow.set_tracking_uri("http://mlflow:5000")

        # Hyperparameter search space for XGBoost
        self.param_distributions = {
            'classifier__n_estimators': [100, 200, 300, 400, 500],
            'classifier__max_depth': [3, 4, 5, 6, 7, 8],
            'classifier__learning_rate': [0.01, 0.05, 0.1, 0.15, 0.2],
            'classifier__subsample': [0.6, 0.7, 0.8, 0.9, 1.0],
            'classifier__colsample_bytree': [0.6, 0.7, 0.8, 0.9, 1.0],
            'classifier__reg_alpha': [0, 0.01, 0.05, 0.1, 0.5],
            'classifier__reg_lambda': [0, 0.01, 0.05, 0.1, 0.5],
            'classifier__min_child_weight': [1, 3, 5, 7],
            'classifier__gamma': [0, 0.1, 0.2, 0.3, 0.4],
            'smote__sampling_strategy': [0.1, 0.2, 0.3, 0.4, 0.5],
            'smote__k_neighbors': [3, 5, 7]
        }

    def load_training_data(self, db_connection, min_samples: int = 1000) -> pd.DataFrame:
        """Load processed transaction data for training"""
        logger.info("📊 Loading training data from database...")

        query = """
        SELECT 
            transaction_id, user_id, amount, merchant_category, payment_method,
            hour_of_day, day_of_week, is_weekend, is_night, is_holiday,
            amount_zscore, amount_percentile, days_since_last,
            transactions_last_1h, transactions_last_24h, transactions_last_7d, transactions_last_30d,
            user_avg_amount, user_std_amount, high_risk_merchant, amount_round,
            geographic_risk, velocity_risk, device_risk, ip_risk_score,
            device_fingerprint_risk, is_fraud, processed_at
        FROM processed_transactions 
        WHERE processed_at >= NOW() - INTERVAL '30 days'
        ORDER BY processed_at DESC
        """

        df = pd.read_sql_query(query, db_connection)

        if len(df) < min_samples:
            logger.warning(f"⚠️ Only {len(df)} samples available, minimum {min_samples} required")
            return pd.DataFrame()

        logger.info(f"✅ Loaded {len(df):,} transactions for training")
        logger.info(f"   Fraud cases: {df['is_fraud'].sum():,} ({df['is_fraud'].mean()*100:.2f}%)")

        return df

    def prepare_features(self, df: pd.DataFrame) -> Tuple[np.ndarray, np.ndarray, List[str]]:
        """Prepare features for ML training"""
        logger.info("🔧 Preparing features for training...")

        # Define feature columns
        numeric_features = [
            'amount', 'hour_of_day', 'day_of_week', 'amount_zscore', 'amount_percentile',
            'days_since_last', 'transactions_last_1h', 'transactions_last_24h',
            'transactions_last_7d', 'transactions_last_30d', 'user_avg_amount',
            'user_std_amount', 'geographic_risk', 'velocity_risk', 'device_risk',
            'ip_risk_score', 'device_fingerprint_risk'
        ]

        boolean_features = [
            'is_weekend', 'is_night', 'is_holiday', 'high_risk_merchant', 'amount_round'
        ]

        categorical_features = ['merchant_category', 'payment_method']

        # Prepare feature matrix
        X_numeric = df[numeric_features].fillna(0).values
        X_boolean = df[boolean_features].fillna(False).astype(int).values

        # Encode categorical features
        X_categorical = []
        categorical_names = []

        for feature in categorical_features:
            if feature not in self.encoders:
                self.encoders[feature] = LabelEncoder()
                encoded = self.encoders[feature].fit_transform(df[feature].fillna('unknown'))
            else:
                encoded = self.encoders[feature].transform(df[feature].fillna('unknown'))

            X_categorical.append(encoded.reshape(-1, 1))
            categorical_names.append(feature)

        # Combine all features
        if X_categorical:
            X_categorical = np.hstack(X_categorical)
            X = np.hstack([X_numeric, X_boolean, X_categorical])
            feature_names = numeric_features + boolean_features + categorical_names
        else:
            X = np.hstack([X_numeric, X_boolean])
            feature_names = numeric_features + boolean_features

        y = df['is_fraud'].values

        logger.info(f"✅ Prepared {X.shape[1]} features for {X.shape[0]} samples")
        logger.info(f"   Feature names: {feature_names}")

        return X, y, feature_names

    def create_ml_pipeline(self) -> ImbPipeline:
        """Create ML pipeline with SMOTE and XGBoost"""
        logger.info("🏗️ Creating ML pipeline with SMOTE + XGBoost...")

        pipeline = ImbPipeline([
            ('scaler', StandardScaler()),
            ('smote', SMOTE(
                sampling_strategy=0.3,  # Increase minority class to 30% of majority
                k_neighbors=5,
                random_state=42,
                n_jobs=-1
            )),
            ('classifier', xgb.XGBClassifier(
                objective='binary:logistic',
                eval_metric='auc',
                use_label_encoder=False,
                random_state=42,
                n_jobs=-1,
                verbosity=0
            ))
        ])

        logger.info("✅ ML pipeline created successfully")
        return pipeline

    def hyperparameter_optimization(self, pipeline: ImbPipeline, X: np.ndarray, y: np.ndarray) -> ImbPipeline:
        """Perform hyperparameter optimization with RandomizedSearchCV"""
        logger.info("🔍 Starting hyperparameter optimization...")

        # Stratified K-Fold for better cross-validation with imbalanced data
        cv_strategy = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)

        # Randomized search with cross-validation
        random_search = RandomizedSearchCV(
            estimator=pipeline,
            param_distributions=self.param_distributions,
            n_iter=50,  # Number of parameter combinations to try
            cv=cv_strategy,
            scoring='roc_auc',  # Optimize for AUC-ROC
            n_jobs=-1,
            random_state=42,
            verbose=1
        )

        logger.info("🚀 Fitting hyperparameter optimization...")
        random_search.fit(X, y)

        logger.info(f"✅ Best parameters found:")
        for param, value in random_search.best_params_.items():
            logger.info(f"   {param}: {value}")

        logger.info(f"✅ Best cross-validation AUC: {random_search.best_score_:.4f}")

        return random_search.best_estimator_

    def evaluate_model(self, model: ImbPipeline, X_test: np.ndarray, y_test: np.ndarray) -> Dict:
        """Comprehensive model evaluation"""
        logger.info("📊 Evaluating model performance...")

        # Predictions
        y_pred = model.predict(X_test)
        y_pred_proba = model.predict_proba(X_test)[:, 1]

        # Calculate metrics
        metrics = {
            'accuracy': accuracy_score(y_test, y_pred),
            'precision': precision_score(y_test, y_pred),
            'recall': recall_score(y_test, y_pred),
            'f1_score': f1_score(y_test, y_pred),
            'roc_auc': roc_auc_score(y_test, y_pred_proba),
            'pr_auc': average_precision_score(y_test, y_pred_proba)
        }

        # Confusion matrix
        cm = confusion_matrix(y_test, y_pred)
        tn, fp, fn, tp = cm.ravel()

        metrics.update({
            'true_negatives': int(tn),
            'false_positives': int(fp),
            'false_negatives': int(fn),
            'true_positives': int(tp),
            'specificity': tn / (tn + fp) if (tn + fp) > 0 else 0,
            'false_positive_rate': fp / (fp + tn) if (fp + tn) > 0 else 0,
            'false_negative_rate': fn / (fn + tp) if (fn + tp) > 0 else 0
        })

        # Log metrics
        logger.info("📈 Model Performance Metrics:")
        logger.info(f"   Accuracy: {metrics['accuracy']:.4f}")
        logger.info(f"   Precision: {metrics['precision']:.4f}")
        logger.info(f"   Recall: {metrics['recall']:.4f}")
        logger.info(f"   F1-Score: {metrics['f1_score']:.4f}")
        logger.info(f"   ROC-AUC: {metrics['roc_auc']:.4f}")
        logger.info(f"   PR-AUC: {metrics['pr_auc']:.4f}")
        logger.info(f"   Specificity: {metrics['specificity']:.4f}")

        logger.info("🎯 Confusion Matrix:")
        logger.info(f"   True Negatives: {tn:,}")
        logger.info(f"   False Positives: {fp:,}")
        logger.info(f"   False Negatives: {fn:,}")
        logger.info(f"   True Positives: {tp:,}")

        return metrics

    def get_feature_importance(self, model: ImbPipeline, feature_names: List[str]) -> Dict:
        """Extract and rank feature importance"""
        logger.info("🔍 Extracting feature importance...")

        try:
            # Get XGBoost feature importance
            xgb_model = model.named_steps['classifier']
            importance_scores = xgb_model.feature_importances_

            # Create feature importance dictionary
            feature_importance = dict(zip(feature_names, importance_scores))

            # Sort by importance
            sorted_features = sorted(feature_importance.items(), key=lambda x: x[1], reverse=True)

            logger.info("🏆 Top 10 Most Important Features:")
            for i, (feature, importance) in enumerate(sorted_features[:10], 1):
                logger.info(f"   {i:2d}. {feature}: {importance:.4f}")

            return dict(sorted_features)

        except Exception as e:
            logger.error(f"Error extracting feature importance: {e}")
            return {}

    def _make_json_serializable(self, obj):
        """Convert complex objects to JSON serializable format"""
        if isinstance(obj, dict):
            return {k: self._make_json_serializable(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._make_json_serializable(item) for item in obj]
        elif isinstance(obj, (int, float, bool, str, type(None))):
            return obj
        elif hasattr(obj, 'item'):  # For numpy types
            return obj.item()
        else:
            return str(obj)

    def save_model_artifacts(self, model: ImbPipeline, metrics: Dict,
                             feature_importance: Dict, run_id: str) -> str:
        """Save model and artifacts"""
        logger.info("💾 Saving model artifacts...")

        # Create model directory
        model_dir = f"/app/models/fraud_detector_{run_id}"
        os.makedirs(model_dir, exist_ok=True)

        # Save model
        model_path = os.path.join(model_dir, "model.pkl")
        joblib.dump(model, model_path)

        # Save metadata
        metadata = {
            'model_type': 'XGBoost with SMOTE',
            'training_timestamp': datetime.now().isoformat(),
            'feature_columns': self.feature_columns,
            'metrics': metrics,
            'feature_importance': feature_importance,
            'run_id': run_id
        }

        metadata_path = os.path.join(model_dir, "metadata.json")
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2, default=str)

        logger.info(f"✅ Model artifacts saved to: {model_dir}")
        return model_path

    def log_to_mlflow(self, model, metrics, feature_importance, hyperparams):
        """Log to MLflow with robust connection handling"""
        logger.info("Logging training run to MLflow...")

        # Generate fallback run_id in case MLflow is unavailable
        fallback_run_id = f"local-{datetime.now().strftime('%Y%m%d%H%M%S')}"

        try:
            mlflow_uri = os.getenv('MLFLOW_TRACKING_URI', 'http://mlflow:5000')
            mlflow.set_tracking_uri(mlflow_uri)

            # Test connection with timeout
            import requests
            from urllib3.util.timeout import Timeout
            try:
                response = requests.get(f"{mlflow_uri}/api/2.0/mlflow/experiments/list",
                                        timeout=3.0)
                if response.status_code != 200:
                    logger.warning(f"MLflow returned status code {response.status_code}")
                    return fallback_run_id
            except Exception as conn_e:
                logger.warning(f"MLflow connection test failed: {conn_e}")
                return fallback_run_id

            # Helper function for serialization
            def _ensure_serializable(obj):
                if isinstance(obj, dict):
                    return {k: _ensure_serializable(v) for k, v in obj.items()}
                elif isinstance(obj, list):
                    return [_ensure_serializable(item) for item in obj]
                elif hasattr(obj, 'item'):  # numpy types
                    return obj.item()
                elif isinstance(obj, (int, float, bool, str, type(None))):
                    return obj
                else:
                    return str(obj)

            # Convert metrics to serializable format
            safe_metrics = _ensure_serializable(metrics)
            safe_params = _ensure_serializable(hyperparams)

            # Start MLflow run
            with mlflow.start_run() as run:
                # Log parameters
                for k, v in safe_params.items():
                    if isinstance(v, (int, float, str, bool)):
                        mlflow.log_param(k, v)

                # Log metrics
                for k, v in safe_metrics.items():
                    if isinstance(v, (int, float)):
                        mlflow.log_metric(k, v)

                # Log feature importance
                for feature, importance in feature_importance.items():
                    try:
                        mlflow.log_metric(f"importance_{feature}", float(importance))
                    except:
                        pass  # Skip if can't convert to float

                # Log model with proper error handling
                try:
                    if hasattr(model, 'named_steps') and 'classifier' in model.named_steps:
                        mlflow.xgboost.log_model(
                            model.named_steps['classifier'],
                            "xgboost_model",
                            registered_model_name="fraud_detection_xgboost"
                        )
                except Exception as model_e:
                    logger.warning(f"Failed to log XGBoost model: {model_e}")

                try:
                    mlflow.sklearn.log_model(
                        model,
                        "complete_pipeline",
                        registered_model_name="fraud_detection_pipeline"
                    )
                except Exception as pipe_e:
                    logger.warning(f"Failed to log pipeline model: {pipe_e}")

                # Return the run ID
                return run.info.run_id

        except Exception as e:
            logger.error(f"Failed to log to MLflow: {e}")
            return fallback_run_id

    def train_model(self, db_connection) -> Dict:
        """Complete model training pipeline"""
        logger.info("🚀 Starting complete ML training pipeline...")
        training_start = datetime.now()

        try:
            # Load data
            df = self.load_training_data(db_connection, min_samples=1000)
            if df.empty:
                raise ValueError("Insufficient training data")

            # Prepare features
            X, y, feature_names = self.prepare_features(df)
            self.feature_columns = feature_names

            # Split data
            X_train, X_test, y_train, y_test = train_test_split(
                X, y, test_size=0.2, random_state=42, stratify=y
            )

            logger.info(f"📊 Training set: {X_train.shape[0]:,} samples")
            logger.info(f"📊 Test set: {X_test.shape[0]:,} samples")
            logger.info(f"📊 Training fraud rate: {y_train.mean()*100:.2f}%")
            logger.info(f"📊 Test fraud rate: {y_test.mean()*100:.2f}%")

            # Create pipeline
            pipeline = self.create_ml_pipeline()

            # Hyperparameter optimization
            best_model = self.hyperparameter_optimization(pipeline, X_train, y_train)

            # Final training on full training set
            logger.info("🏋️ Training final model on full training set...")
            best_model.fit(X_train, y_train)

            # Evaluate model
            metrics = self.evaluate_model(best_model, X_test, y_test)

            # Feature importance
            feature_importance = self.get_feature_importance(best_model, feature_names)

            # Get best hyperparameters
            hyperparams = best_model.get_params()

            # Log to MLflow
            mlflow_run_id = self.log_to_mlflow(best_model, metrics, feature_importance, hyperparams)

            # Save model artifacts
            model_path = self.save_model_artifacts(best_model, metrics, feature_importance, mlflow_run_id)

            training_end = datetime.now()
            training_duration = training_end - training_start

            # Save training run to database
            self.save_training_run_to_db(
                db_connection, mlflow_run_id, metrics, feature_importance,
                hyperparams, training_start, training_end, model_path,
                len(df), y.sum()
            )

            logger.info(f"🎉 Model training completed successfully!")
            logger.info(f"⏱️ Training duration: {training_duration}")
            logger.info(f"📂 Model saved to: {model_path}")
            logger.info(f"📝 MLflow run ID: {mlflow_run_id}")

            return {
                'status': 'success',
                'mlflow_run_id': mlflow_run_id,
                'model_path': model_path,
                'metrics': metrics,
                'training_duration': str(training_duration),
                'training_samples': len(df),
                'fraud_samples': int(y.sum())
            }

        except Exception as e:
            logger.error(f"❌ Model training failed: {e}")
            return {
                'status': 'failed',
                'error': str(e),
                'training_duration': str(datetime.now() - training_start)
            }

    def save_training_run_to_db(self, db_connection, run_id, metrics, feature_importance, hyperparams,
                                training_start, training_end, model_path, total_samples, fraud_samples):
        """Save training run to database with proper serialization"""
        try:
            # Helper function for JSON serialization of complex objects
            def make_serializable(obj):
                if isinstance(obj, dict):
                    return {k: make_serializable(v) for k, v in obj.items()}
                elif isinstance(obj, list):
                    return [make_serializable(item) for item in obj]
                elif hasattr(obj, 'item'): # For numpy types
                    return obj.item()
                elif hasattr(obj, '__dict__'): # For objects
                    return str(obj)
                elif isinstance(obj, (int, float, str, bool, type(None))):
                    return obj
                else:
                    return str(obj)

            cursor = db_connection.cursor()

            # Serialize complex objects
            serializable_hyperparams = make_serializable(hyperparams)
            serializable_feature_importance = make_serializable(feature_importance)

            # Insert with proper serialization
            cursor.execute("""
            INSERT INTO model_training_runs (
                run_id, model_name, model_version, algorithm, training_data_count,
                training_fraud_count, training_fraud_rate, accuracy, precision_score,
                recall_score, f1_score, auc_roc, hyperparameters,
                feature_importance, training_start, training_end, model_path, 
                status, mlflow_run_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                run_id, 'fraud_detection_xgboost', 'v1.0', 'XGBoost with SMOTE',
                total_samples, fraud_samples, fraud_samples/total_samples if total_samples > 0 else 0,
                metrics.get('accuracy', 0), metrics.get('precision', 0),
                metrics.get('recall', 0), metrics.get('f1_score', 0),
                metrics.get('roc_auc', 0),
                json.dumps(serializable_hyperparams),
                json.dumps(serializable_feature_importance),
                training_start, training_end, model_path,
                'completed', run_id
            ))

            db_connection.commit()
            print("Training run saved to database successfully")

        except Exception as e:
            print(f"Failed to save training run to database: {e}")
            import traceback
            traceback.print_exc()
            if db_connection:
                db_connection.rollback()


# Initialize ML Pipeline
ml_pipeline = FraudDetectionMLPipeline()

# ========================================
# CELERY TASKS
# ========================================

@celery_app.task(bind=True, base=MLTrainingTask, name='train_fraud_detection_model')
def train_fraud_detection_model(self) -> Dict:
    """Main task for training fraud detection model"""
    logger.info("🎯 Starting fraud detection model training task...")

    try:
        result = ml_pipeline.train_model(self.db_connection)

        # Store result in Redis for monitoring
        redis_client.setex(
            f"training_result_{self.request.id}",
            3600,  # 1 hour expiry
            json.dumps(result, default=str)
        )

        return result

    except Exception as e:
        logger.error(f"Training task failed: {e}")
        error_result = {
            'status': 'failed',
            'error': str(e),
            'task_id': self.request.id
        }

        redis_client.setex(
            f"training_result_{self.request.id}",
            3600,
            json.dumps(error_result)
        )

        return error_result

@celery_app.task(bind=True, base=MLTrainingTask, name='evaluate_model_performance')
def evaluate_model_performance(self, model_path: str) -> Dict:
    """Task to evaluate model performance on recent data"""
    logger.info(f"📊 Evaluating model performance: {model_path}")

    try:
        # Load model
        model = joblib.load(model_path)

        # Load recent data for evaluation
        query = """
        SELECT * FROM processed_transactions 
        WHERE processed_at >= NOW() - INTERVAL '7 days'
        AND is_fraud IS NOT NULL
        """

        df = pd.read_sql_query(query, self.db_connection)

        if len(df) < 100:
            return {'status': 'insufficient_data', 'samples': len(df)}

        # Prepare features (reuse the same preparation logic)
        X, y, _ = ml_pipeline.prepare_features(df)

        # Evaluate
        metrics = ml_pipeline.evaluate_model(model, X, y)

        logger.info("✅ Model evaluation completed")
        return {'status': 'success', 'metrics': metrics, 'evaluation_samples': len(df)}

    except Exception as e:
        logger.error(f"Model evaluation failed: {e}")
        return {'status': 'failed', 'error': str(e)}

@celery_app.task(bind=True, base=MLTrainingTask, name='cleanup_old_models')
def cleanup_old_models(self, keep_latest: int = 5) -> Dict:
    """Task to cleanup old model files and database records"""
    logger.info(f"🧹 Cleaning up old models, keeping latest {keep_latest}")

    try:
        cursor = self.db_connection.cursor()

        # Get old training runs
        cursor.execute("""
        SELECT id, model_path FROM model_training_runs 
        WHERE status = 'completed'
        ORDER BY created_at DESC 
        OFFSET %s
        """, (keep_latest,))

        old_runs = cursor.fetchall()
        deleted_files = 0
        deleted_records = 0

        for run_id, model_path in old_runs:
            # Delete model files
            try:
                if model_path and os.path.exists(model_path):
                    os.remove(model_path)
                    # Also remove the directory if empty
                    model_dir = os.path.dirname(model_path)
                    if os.path.exists(model_dir) and not os.listdir(model_dir):
                        os.rmdir(model_dir)
                    deleted_files += 1
            except Exception as e:
                logger.warning(f"Failed to delete model file {model_path}: {e}")

            # Delete database record
            try:
                cursor.execute("DELETE FROM model_training_runs WHERE id = %s", (run_id,))
                deleted_records += 1
            except Exception as e:
                logger.warning(f"Failed to delete database record {run_id}: {e}")

        self.db_connection.commit()
        cursor.close()

        logger.info(f"✅ Cleanup completed: {deleted_files} files, {deleted_records} records deleted")
        return {
            'status': 'success',
            'deleted_files': deleted_files,
            'deleted_records': deleted_records
        }

    except Exception as e:
        logger.error(f"Cleanup failed: {e}")
        return {'status': 'failed', 'error': str(e)}

# ========================================
# CELERY BEAT SCHEDULE
# ========================================

celery_app.conf.beat_schedule = {
    # Train model every 6 hours
    'train-fraud-model': {
        'task': 'train_fraud_detection_model',
        'schedule': crontab(minute=0, hour='*/6'),  # Every 6 hours
    },

    # Evaluate model performance daily
    'evaluate-model-performance': {
        'task': 'evaluate_model_performance',
        'schedule': crontab(minute=30, hour=2),  # Daily at 2:30 AM
        'kwargs': {'model_path': '/app/models/latest/model.pkl'}
    },

    # Cleanup old models weekly
    'cleanup-old-models': {
        'task': 'cleanup_old_models',
        'schedule': crontab(minute=0, hour=3, day_of_week=1),  # Weekly on Monday at 3 AM
        'kwargs': {'keep_latest': 10}
    }
}

celery_app.conf.timezone = 'UTC'