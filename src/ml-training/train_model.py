# ml-training/train_model.py
import os
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, Any, Tuple, Optional
import joblib
import json
from sklearn.model_selection import train_test_split, RandomizedSearchCV
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.metrics import classification_report, confusion_matrix, roc_auc_score, precision_recall_curve
from imblearn.over_sampling import SMOTE
import xgboost as xgb
import psycopg2
import psycopg2.extras
from loguru import logger

class FraudDetectionTrainer:
    def __init__(self):
        # Database configuration
        self.db_config = {
            'host': os.getenv('POSTGRES_HOST', 'localhost'),
            'port': int(os.getenv('POSTGRES_PORT', 5432)),
            'database': os.getenv('POSTGRES_DB', 'fraud_detection'),
            'user': os.getenv('POSTGRES_USER', 'mluser'),
            'password': os.getenv('POSTGRES_PASSWORD', 'mlpassword')
        }

        # Model configuration
        self.model_name = os.getenv('MODEL_NAME', 'xgboost_fraud_detector')
        self.model_version = os.getenv('MODEL_VERSION', '1.0')
        self.models_dir = '/app/models'
        self.data_dir = '/app/data'

        # Training configuration
        self.test_size = float(os.getenv('TEST_SIZE', '0.2'))
        self.random_state = int(os.getenv('RANDOM_STATE', '42'))
        self.cv_folds = int(os.getenv('CV_FOLDS', '5'))
        self.n_iter_search = int(os.getenv('N_ITER_SEARCH', '20'))

        # Ensure directories exist
        os.makedirs(self.models_dir, exist_ok=True)
        os.makedirs(self.data_dir, exist_ok=True)

        # Model performance tracking
        self.training_metrics = {}

        logger.info("FraudDetectionTrainer initialized")

    def load_data_from_db(self, limit: Optional[int] = None) -> pd.DataFrame:
        """Load processed transaction data from PostgreSQL"""
        try:
            conn = psycopg2.connect(**self.db_config)

            query = """
                SELECT 
                    transaction_id, user_id, amount, merchant_category,
                    hour_of_day, day_of_week, is_weekend, is_night,
                    amount_log, amount_round, amount_zscore,
                    high_risk_merchant, merchant_risk_score,
                    transactions_last_24h, transactions_last_7d,
                    user_avg_amount_7d, minutes_since_last,
                    user_merchant_frequency, user_hour_frequency,
                    anomaly_score, amount_unusual, velocity_unusual,
                    is_fraud
                FROM processed_transactions
                WHERE timestamp >= NOW() - INTERVAL '30 days'
                ORDER BY timestamp DESC
            """

            if limit:
                query += f" LIMIT {limit}"

            df = pd.read_sql_query(query, conn)
            conn.close()

            logger.info(f"Loaded {len(df)} transactions from database")
            logger.info(f"Fraud rate: {df['is_fraud'].mean()*100:.2f}%")

            return df

        except Exception as e:
            logger.error(f"Failed to load data from database: {e}")
            raise

    def prepare_features(self, df: pd.DataFrame) -> Tuple[np.ndarray, np.ndarray, list]:
        """Prepare features for model training"""
        # Define feature columns
        categorical_features = ['merchant_category']
        boolean_features = [
            'is_weekend', 'is_night', 'amount_round', 'high_risk_merchant',
            'amount_unusual', 'velocity_unusual'
        ]
        numerical_features = [
            'amount', 'hour_of_day', 'day_of_week', 'amount_log', 'amount_zscore',
            'merchant_risk_score', 'transactions_last_24h', 'transactions_last_7d',
            'user_avg_amount_7d', 'minutes_since_last', 'user_merchant_frequency',
            'user_hour_frequency', 'anomaly_score'
        ]

        # Handle missing values
        df = df.fillna({
            'user_avg_amount_7d': df['amount'].mean(),
            'amount_zscore': 0.0,
            'minutes_since_last': df['minutes_since_last'].median(),
            'user_merchant_frequency': 1,
            'user_hour_frequency': 1,
            'anomaly_score': 0.0
        })

        # Encode categorical features
        feature_df = df.copy()
        label_encoders = {}

        for cat_feature in categorical_features:
            le = LabelEncoder()
            feature_df[cat_feature + '_encoded'] = le.fit_transform(feature_df[cat_feature].astype(str))
            label_encoders[cat_feature] = le

        # Convert boolean features to int
        for bool_feature in boolean_features:
            feature_df[bool_feature] = feature_df[bool_feature].astype(int)

        # Final feature list
        final_features = (
                numerical_features +
                [f + '_encoded' for f in categorical_features] +
                boolean_features
        )

        # Prepare feature matrix
        X = feature_df[final_features].values
        y = feature_df['is_fraud'].values

        # Handle infinite values
        X = np.where(np.isinf(X), 0, X)

        logger.info(f"Prepared features: {len(final_features)} features, {len(X)} samples")
        logger.info(f"Feature names: {final_features}")

        return X, y, final_features

    def balance_dataset(self, X: np.ndarray, y: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
        """Balance dataset using SMOTE"""
        try:
            # Check class distribution
            unique, counts = np.unique(y, return_counts=True)
            logger.info(f"Original class distribution: {dict(zip(unique, counts))}")

            # Apply SMOTE if dataset is imbalanced
            fraud_ratio = np.sum(y) / len(y)
            if fraud_ratio < 0.1:  # Less than 10% fraud
                smote = SMOTE(
                    sampling_strategy=0.1,  # Target 10% fraud ratio
                    random_state=self.random_state,
                    k_neighbors=min(5, np.sum(y) - 1)  # Ensure we have enough neighbors
                )
                X_balanced, y_balanced = smote.fit_resample(X, y)

                unique, counts = np.unique(y_balanced, return_counts=True)
                logger.info(f"Balanced class distribution: {dict(zip(unique, counts))}")

                return X_balanced, y_balanced
            else:
                logger.info("Dataset already reasonably balanced, skipping SMOTE")
                return X, y

        except Exception as e:
            logger.warning(f"SMOTE failed, using original dataset: {e}")
            return X, y

    def get_xgboost_param_grid(self) -> Dict[str, Any]:
        """Get XGBoost hyperparameter search space"""
        return {
            'n_estimators': [100, 200, 300],
            'max_depth': [3, 4, 5, 6],
            'learning_rate': [0.01, 0.1, 0.2],
            'subsample': [0.8, 0.9, 1.0],
            'colsample_bytree': [0.8, 0.9, 1.0],
            'min_child_weight': [1, 3, 5],
            'gamma': [0, 0.1, 0.2],
            'reg_alpha': [0, 0.01, 0.1],
            'reg_lambda': [1, 1.1, 1.2]
        }

    def train_model(self, X: np.ndarray, y: np.ndarray, feature_names: list) -> Tuple[xgb.XGBClassifier, Dict[str, Any]]:
        """Train XGBoost model with hyperparameter tuning"""
        logger.info("Starting model training with hyperparameter tuning...")

        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=self.test_size, random_state=self.random_state,
            stratify=y if np.sum(y) > 10 else None
        )

        # Base XGBoost model
        base_model = xgb.XGBClassifier(
            objective='binary:logistic',
            eval_metric='auc',
            random_state=self.random_state,
            n_jobs=-1
        )

        # Hyperparameter search
        param_grid = self.get_xgboost_param_grid()

        search = RandomizedSearchCV(
            base_model,
            param_distributions=param_grid,
            n_iter=self.n_iter_search,
            cv=min(self.cv_folds, 3),  # Reduce CV folds if small dataset
            scoring='roc_auc',
            random_state=self.random_state,
            n_jobs=-1,
            verbose=1
        )

        # Fit the search
        search.fit(X_train, y_train)
        best_model = search.best_estimator_

        logger.info(f"Best parameters: {search.best_params_}")
        logger.info(f"Best cross-validation AUC: {search.best_score_:.4f}")

        # Evaluate on test set
        y_pred = best_model.predict(X_test)
        y_pred_proba = best_model.predict_proba(X_test)[:, 1]

        # Calculate metrics
        test_auc = roc_auc_score(y_test, y_pred_proba)
        classification_rep = classification_report(y_test, y_pred, output_dict=True)

        metrics = {
            'best_params': search.best_params_,
            'cv_auc': search.best_score_,
            'test_auc': test_auc,
            'precision': classification_rep['1']['precision'],
            'recall': classification_rep['1']['recall'],
            'f1_score': classification_rep['1']['f1-score'],
            'support': classification_rep['1']['support'],
            'training_samples': len(X_train),
            'test_samples': len(X_test),
            'feature_count': len(feature_names)
        }

        # Feature importance
        feature_importance = dict(zip(feature_names, best_model.feature_importances_))
        metrics['feature_importance'] = feature_importance

        logger.info(f"Model training completed:")
        logger.info(f"  - Test AUC: {test_auc:.4f}")
        logger.info(f"  - Precision: {metrics['precision']:.4f}")
        logger.info(f"  - Recall: {metrics['recall']:.4f}")
        logger.info(f"  - F1-Score: {metrics['f1_score']:.4f}")

        # Log top features
        top_features = sorted(feature_importance.items(), key=lambda x: x[1], reverse=True)[:10]
        logger.info("Top 10 features:")
        for feature, importance in top_features:
            logger.info(f"  - {feature}: {importance:.4f}")

        self.training_metrics = metrics
        return best_model, metrics

    def save_model(self, model: xgb.XGBClassifier, metrics: Dict[str, Any], feature_names: list) -> str:
        """Save model and metadata"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        model_filename = f"{self.model_name}_v{self.model_version}_{timestamp}.joblib"
        model_path = os.path.join(self.models_dir, model_filename)

        # Save model
        joblib.dump(model, model_path)

        # Save metadata
        metadata = {
            'model_name': self.model_name,
            'model_version': self.model_version,
            'timestamp': timestamp,
            'model_path': model_path,
            'feature_names': feature_names,
            'metrics': metrics,
            'training_config': {
                'test_size': self.test_size,
                'random_state': self.random_state,
                'cv_folds': self.cv_folds,
                'n_iter_search': self.n_iter_search
            }
        }

        metadata_path = model_path.replace('.joblib', '_metadata.json')
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2, default=str)

        logger.info(f"Model saved to: {model_path}")
        logger.info(f"Metadata saved to: {metadata_path}")

        return model_path

    def save_training_run_to_db(self, model_path: str, metrics: Dict[str, Any]):
        """Save training run information to database"""
        try:
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()

            insert_query = """
                INSERT INTO model_training_runs (
                    run_id, model_name, model_version, training_data_count,
                    fraud_count, accuracy, precision_score, recall_score, 
                    f1_score, auc_score, training_start, training_end,
                    model_path, status
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            """

            run_id = f"{self.model_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

            cursor.execute(insert_query, (
                run_id,
                self.model_name,
                self.model_version,
                metrics['training_samples'] + metrics['test_samples'],
                int(metrics['support']),
                0.0,  # We don't have accuracy metric
                metrics['precision'],
                metrics['recall'],
                metrics['f1_score'],
                metrics['test_auc'],
                datetime.now(),
                datetime.now(),
                model_path,
                'completed'
            ))

            conn.commit()
            cursor.close()
            conn.close()

            logger.info(f"Training run saved to database: {run_id}")

        except Exception as e:
            logger.error(f"Failed to save training run to database: {e}")

    def train_full_pipeline(self, data_limit: Optional[int] = None) -> Dict[str, Any]:
        """Run the complete training pipeline"""
        try:
            logger.info("Starting full training pipeline...")
            start_time = datetime.now()

            # Load data
            df = self.load_data_from_db(limit=data_limit)

            if len(df) < 100:
                raise ValueError("Insufficient data for training (minimum 100 samples required)")

            # Prepare features
            X, y, feature_names = self.prepare_features(df)

            # Balance dataset
            X_balanced, y_balanced = self.balance_dataset(X, y)

            # Train model
            model, metrics = self.train_model(X_balanced, y_balanced, feature_names)

            # Save model
            model_path = self.save_model(model, metrics, feature_names)

            # Save to database
            self.save_training_run_to_db(model_path, metrics)

            end_time = datetime.now()
            training_duration = (end_time - start_time).total_seconds()

            result = {
                'status': 'success',
                'model_path': model_path,
                'metrics': metrics,
                'training_duration': training_duration,
                'data_samples': len(df),
                'fraud_samples': int(np.sum(y)),
                'start_time': start_time.isoformat(),
                'end_time': end_time.isoformat()
            }

            logger.info(f"Training pipeline completed in {training_duration:.2f} seconds")
            return result

        except Exception as e:
            logger.error(f"Training pipeline failed: {e}")
            return {
                'status': 'failed',
                'error': str(e),
                'end_time': datetime.now().isoformat()
            }

# Example usage and testing
if __name__ == "__main__":
    trainer = FraudDetectionTrainer()

    # Run training pipeline
    result = trainer.train_full_pipeline(data_limit=10000)

    print("Training Result:")
    print(json.dumps(result, indent=2, default=str))