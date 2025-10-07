"""
GPS Kalman Filter for LoRa Adapter

Implements a Kalman filter to smooth GPS coordinates (latitude, longitude)
from noisy sensor readings.
"""
import numpy as np
import logging
from typing import Tuple, Optional, Dict, Any

logger = logging.getLogger(__name__)


class GPSKalmanFilter:
    """
    Kalman filter for GPS coordinates (latitude, longitude)
    
    State vector: [lat, lon, lat_velocity, lon_velocity]
    Measurement vector: [lat, lon]
    """
    
    def __init__(self, 
                 process_noise: float = 1e-4,
                 measurement_noise: float = 1e-3,
                 initial_uncertainty: float = 1e-2):
        """
        Initialize GPS Kalman filter
        
        Args:
            process_noise: Process noise variance (how much we expect the GPS to move)
            measurement_noise: Measurement noise variance (GPS sensor accuracy)
            initial_uncertainty: Initial state uncertainty
        """
        self.process_noise = process_noise
        self.measurement_noise = measurement_noise
        self.initial_uncertainty = initial_uncertainty
        
        # State dimension (lat, lon, lat_vel, lon_vel)
        self.state_dim = 4
        # Measurement dimension (lat, lon)
        self.measurement_dim = 2
        
        # State vector [lat, lon, lat_velocity, lon_velocity]
        self.x = np.zeros(self.state_dim)
        
        # State covariance matrix
        self.P = np.eye(self.state_dim) * initial_uncertainty
        
        # State transition model (assumes constant velocity)
        self.F = np.array([
            [1, 0, 1, 0],  # lat = lat + lat_velocity
            [0, 1, 0, 1],  # lon = lon + lon_velocity  
            [0, 0, 1, 0],  # lat_velocity = lat_velocity
            [0, 0, 0, 1]   # lon_velocity = lon_velocity
        ])
        
        # Process noise covariance
        self.Q = np.array([
            [process_noise, 0, 0, 0],
            [0, process_noise, 0, 0],
            [0, 0, process_noise, 0],
            [0, 0, 0, process_noise]
        ])
        
        # Measurement model (we observe lat, lon directly)
        self.H = np.array([
            [1, 0, 0, 0],  # measure lat
            [0, 1, 0, 0]   # measure lon
        ])
        
        # Measurement noise covariance
        self.R = np.eye(self.measurement_dim) * measurement_noise
        
        self.initialized = False
        self.last_update_time = None
        
    def update(self, latitude: float, longitude: float, timestamp: Optional[float] = None) -> Tuple[float, float]:
        """
        Update Kalman filter with new GPS measurement
        
        Args:
            latitude: Measured latitude
            longitude: Measured longitude  
            timestamp: Measurement timestamp (optional)
            
        Returns:
            Tuple of (filtered_latitude, filtered_longitude)
        """
        try:
            # Measurement vector
            z = np.array([latitude, longitude])
            
            if not self.initialized:
                # Initialize state with first measurement
                self.x[0] = latitude   # lat
                self.x[1] = longitude  # lon
                self.x[2] = 0.0       # lat_velocity
                self.x[3] = 0.0       # lon_velocity
                self.initialized = True
                self.last_update_time = timestamp
                
                logger.debug(f"GPS Kalman filter initialized: lat={latitude:.6f}, lon={longitude:.6f}")
                return latitude, longitude
            
            # Predict step
            self.x = self.F @ self.x
            self.P = self.F @ self.P @ self.F.T + self.Q
            
            # Update step
            # Innovation (measurement residual)
            y = z - self.H @ self.x
            
            # Innovation covariance
            S = self.H @ self.P @ self.H.T + self.R
            
            # Kalman gain
            K = self.P @ self.H.T @ np.linalg.inv(S)
            
            # Update state and covariance
            self.x = self.x + K @ y
            I = np.eye(self.state_dim)
            self.P = (I - K @ self.H) @ self.P
            
            filtered_lat = float(self.x[0])
            filtered_lon = float(self.x[1])
            
            # Log the filtering effect
            lat_diff = abs(filtered_lat - latitude)
            lon_diff = abs(filtered_lon - longitude)
            if lat_diff > 1e-6 or lon_diff > 1e-6:
                logger.debug(f"GPS filtered: "
                           f"lat {latitude:.6f}->{filtered_lat:.6f} (Δ{lat_diff:.6f}), "
                           f"lon {longitude:.6f}->{filtered_lon:.6f} (Δ{lon_diff:.6f})")
            
            self.last_update_time = timestamp
            return filtered_lat, filtered_lon
            
        except Exception as e:
            logger.error(f"GPS Kalman filter error: {e}")
            # Fallback to raw measurements
            return latitude, longitude
    
    def reset(self):
        """Reset the filter state"""
        self.x = np.zeros(self.state_dim)
        self.P = np.eye(self.state_dim) * self.initial_uncertainty
        self.initialized = False
        self.last_update_time = None
        logger.debug("GPS Kalman filter reset")
    
    def get_velocity(self) -> Tuple[float, float]:
        """Get estimated velocity in lat/lon per time unit"""
        if not self.initialized:
            return 0.0, 0.0
        return float(self.x[2]), float(self.x[3])
    
    def get_position_uncertainty(self) -> Tuple[float, float]:
        """Get position uncertainty (standard deviation)"""
        if not self.initialized:
            return self.initial_uncertainty, self.initial_uncertainty
        return float(np.sqrt(self.P[0, 0])), float(np.sqrt(self.P[1, 1]))


class DeviceGPSFilterManager:
    """
    Manages GPS Kalman filters for multiple devices
    """
    
    def __init__(self, 
                 process_noise: float = 1e-4,
                 measurement_noise: float = 1e-3,
                 initial_uncertainty: float = 1e-2,
                 max_filters: int = 1000):
        """
        Initialize GPS filter manager
        
        Args:
            process_noise: Process noise for all filters
            measurement_noise: Measurement noise for all filters  
            initial_uncertainty: Initial uncertainty for all filters
            max_filters: Maximum number of device filters to maintain
        """
        self.process_noise = process_noise
        self.measurement_noise = measurement_noise
        self.initial_uncertainty = initial_uncertainty
        self.max_filters = max_filters
        
        # Dictionary to store filters per device
        self.filters: Dict[str, GPSKalmanFilter] = {}
        
    def filter_gps(self, device_id: str, latitude: float, longitude: float, 
                   timestamp: Optional[float] = None) -> Tuple[float, float]:
        """
        Apply Kalman filtering to GPS coordinates for a specific device
        
        Args:
            device_id: Unique device identifier (e.g., DevEUI)
            latitude: Raw latitude measurement
            longitude: Raw longitude measurement
            timestamp: Measurement timestamp
            
        Returns:
            Tuple of (filtered_latitude, filtered_longitude)
        """
        # Get or create filter for this device
        if device_id not in self.filters:
            if len(self.filters) >= self.max_filters:
                # Remove oldest filter (simple FIFO)
                oldest_device = next(iter(self.filters))
                del self.filters[oldest_device]
                logger.debug(f"Removed GPS filter for device {oldest_device} (max filters reached)")
            
            self.filters[device_id] = GPSKalmanFilter(
                process_noise=self.process_noise,
                measurement_noise=self.measurement_noise,
                initial_uncertainty=self.initial_uncertainty
            )
            logger.debug(f"Created GPS filter for device {device_id}")
        
        # Apply filtering
        return self.filters[device_id].update(latitude, longitude, timestamp)
    
    def reset_device_filter(self, device_id: str):
        """Reset filter for a specific device"""
        if device_id in self.filters:
            self.filters[device_id].reset()
    
    def remove_device_filter(self, device_id: str):
        """Remove filter for a specific device"""
        if device_id in self.filters:
            del self.filters[device_id]
            logger.debug(f"Removed GPS filter for device {device_id}")
    
    def get_device_count(self) -> int:
        """Get number of active device filters"""
        return len(self.filters)