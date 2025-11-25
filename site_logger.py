"""
Site Logger Module for Basketball Analytics Dashboard
Tracks visitors and their interactions with the dashboard
"""

import os
import time
from datetime import datetime
from typing import Dict, List, Any
import streamlit as st
import hashlib
import uuid

class SiteLogger:
    """Simple site logger to track visitors and interactions"""
    
    def __init__(self, log_file: str = "site_logs.txt"):
        self.log_file = log_file
        self.session_id = self._get_or_create_session_id()
        self.visitor_id = self._get_or_create_visitor_id()
        
    def _get_or_create_session_id(self) -> str:
        """Get or create a unique session ID"""
        if 'session_id' not in st.session_state:
            st.session_state.session_id = str(uuid.uuid4())
        return st.session_state.session_id
    
    def _get_or_create_visitor_id(self) -> str:
        """Get or create a unique visitor ID based on IP and user agent"""
        if 'visitor_id' not in st.session_state:
            # Create a simple hash based on available info
            user_info = f"{st.get_option('server.headless')}_{datetime.now().strftime('%Y%m%d')}"
            st.session_state.visitor_id = hashlib.md5(user_info.encode()).hexdigest()[:12]
        return st.session_state.visitor_id
    
    def _get_client_info(self) -> Dict[str, Any]:
        """Get basic client information"""
        return {
            "timestamp": datetime.now().isoformat(),
            "session_id": self.session_id,
            "visitor_id": self.visitor_id,
            "page": "Basketball Analytics Dashboard"
        }
    
    def log_page_visit(self, page_name: str = "Dashboard"):
        """Log when a user visits a page"""
        log_entry = {
            **self._get_client_info(),
            "action": "page_visit",
            "page_name": page_name,
            "details": f"User visited {page_name}"
        }
        self._write_log(log_entry)
    
    def log_interaction(self, interaction_type: str, details: str, data: Dict[str, Any] = None):
        """Log user interactions like clicks, selections, etc."""
        log_entry = {
            **self._get_client_info(),
            "action": "interaction",
            "interaction_type": interaction_type,
            "details": details,
            "data": data or {}
        }
        self._write_log(log_entry)
    
    def log_filter_change(self, filter_name: str, old_value: Any, new_value: Any):
        """Log when user changes filters"""
        self.log_interaction(
            "filter_change",
            f"Changed {filter_name} from '{old_value}' to '{new_value}'",
            {
                "filter_name": filter_name,
                "old_value": str(old_value),
                "new_value": str(new_value)
            }
        )
    
    def log_visualization_view(self, viz_name: str, viz_type: str = "chart"):
        """Log when user views a visualization"""
        self.log_interaction(
            "visualization_view",
            f"Viewed {viz_name} ({viz_type})",
            {
                "visualization_name": viz_name,
                "visualization_type": viz_type
            }
        )
    
    def log_data_export(self, export_type: str, data_size: int = None):
        """Log when user exports data"""
        self.log_interaction(
            "data_export",
            f"Exported {export_type}",
            {
                "export_type": export_type,
                "data_size": data_size
            }
        )
    
    def _write_log(self, log_entry: Dict[str, Any]):
        """Write log entry to text file"""
        try:
            # Format log entry as readable text
            timestamp = log_entry.get('timestamp', datetime.now().isoformat())
            action = log_entry.get('action', 'unknown')
            details = log_entry.get('details', '')
            visitor_id = log_entry.get('visitor_id', 'unknown')
            session_id = log_entry.get('session_id', 'unknown')
            
            # Create log line
            log_line = f"[{timestamp}] Visitor: {visitor_id[:8]} | Session: {session_id[:8]} | Action: {action} | {details}\n"
            
            # Append to log file
            with open(self.log_file, 'a', encoding='utf-8') as f:
                f.write(log_line)
            
            # Keep file size manageable by rotating if it gets too large
            if os.path.exists(self.log_file) and os.path.getsize(self.log_file) > 1024 * 1024:  # 1MB
                self._rotate_log_file()
                
        except Exception as e:
            # Silently fail to not disrupt the main app
            print(f"Logging error: {e}")
    
    def _rotate_log_file(self):
        """Rotate log file when it gets too large"""
        try:
            if os.path.exists(self.log_file):
                # Read last 500 lines
                with open(self.log_file, 'r', encoding='utf-8') as f:
                    lines = f.readlines()
                
                # Keep only last 500 lines
                if len(lines) > 500:
                    with open(self.log_file, 'w', encoding='utf-8') as f:
                        f.writelines(lines[-500:])
        except Exception as e:
            print(f"Log rotation error: {e}")
    
    def get_visitor_stats(self) -> Dict[str, Any]:
        """Get basic visitor statistics from text log file"""
        try:
            if not os.path.exists(self.log_file):
                return {"total_visits": 0, "unique_visitors": 0, "recent_activity": []}
            
            with open(self.log_file, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            
            # Parse log lines
            visits = 0
            visitors = set()
            recent_activity = []
            
            for line in lines[-50:]:  # Last 50 lines for recent activity
                if "Action: page_visit" in line:
                    visits += 1
                if "Visitor:" in line:
                    visitor_id = line.split("Visitor: ")[1].split(" |")[0]
                    visitors.add(visitor_id)
                recent_activity.append(line.strip())
            
            return {
                "total_visits": visits,
                "unique_visitors": len(visitors),
                "recent_activity": recent_activity[-10:],  # Last 10 entries
                "total_logs": len(lines)
            }
            
        except Exception as e:
            return {"error": str(e), "total_visits": 0, "unique_visitors": 0, "recent_activity": []}
    
    def get_interaction_summary(self) -> Dict[str, int]:
        """Get summary of interaction types from text log file"""
        try:
            if not os.path.exists(self.log_file):
                return {}
            
            with open(self.log_file, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            
            # Count interaction types
            interaction_counts = {}
            for line in lines:
                if "Action: interaction" in line and "interaction_type:" in line:
                    # Extract interaction type from line
                    if "filter_change" in line:
                        interaction_counts["filter_change"] = interaction_counts.get("filter_change", 0) + 1
                    elif "visualization_view" in line:
                        interaction_counts["visualization_view"] = interaction_counts.get("visualization_view", 0) + 1
                    elif "data_export" in line:
                        interaction_counts["data_export"] = interaction_counts.get("data_export", 0) + 1
                    else:
                        interaction_counts["other"] = interaction_counts.get("other", 0) + 1
            
            return interaction_counts
            
        except Exception as e:
            return {"error": str(e)}
    
    def clear_logs(self):
        """Clear all logs (admin function)"""
        try:
            if os.path.exists(self.log_file):
                os.remove(self.log_file)
            return True
        except Exception as e:
            return False

# Global logger instance
logger = SiteLogger()

def log_page_visit(page_name: str = "Dashboard"):
    """Convenience function to log page visits"""
    logger.log_page_visit(page_name)

def log_interaction(interaction_type: str, details: str, data: Dict[str, Any] = None):
    """Convenience function to log interactions"""
    logger.log_interaction(interaction_type, details, data)

def log_filter_change(filter_name: str, old_value: Any, new_value: Any):
    """Convenience function to log filter changes"""
    logger.log_filter_change(filter_name, old_value, new_value)

def log_visualization_view(viz_name: str, viz_type: str = "chart"):
    """Convenience function to log visualization views"""
    logger.log_visualization_view(viz_name, viz_type)

def get_visitor_stats():
    """Convenience function to get visitor statistics"""
    return logger.get_visitor_stats()

def get_interaction_summary():
    """Convenience function to get interaction summary"""
    return logger.get_interaction_summary()
