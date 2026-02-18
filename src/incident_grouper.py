"""
Incident Grouping Logic
Groups HDFS logs into incidents based on block_id and time proximity
"""

import json
from datetime import datetime, timedelta
from typing import List, Dict
from collections import defaultdict


class IncidentGrouper:
    """
    Groups logs into incidents.
    
    Rule: Logs with same block_id within time_window_minutes are one incident
    """
    
    def __init__(self, time_window_minutes: int = 5):
        """
        Args:
            time_window_minutes: Time window for grouping logs (default 5 min)
        """
        self.time_window = timedelta(minutes=time_window_minutes)
    
    def group_incidents(self, logs: List[Dict]) -> List[Dict]:
        """
        Group logs into incidents.
        
        Args:
            logs: List of parsed log dictionaries
            
        Returns:
            List of incidents, each containing grouped logs
        """
        # Filter logs with block_id (logs without block_id can't be grouped)
        logs_with_blocks = [log for log in logs if log.get('block_id')]
        print(f"\nGrouping {len(logs_with_blocks)} logs with block IDs into incidents...")
        
        # Group by block_id first
        blocks = defaultdict(list)
        for log in logs_with_blocks:
            blocks[log['block_id']].append(log)
        
        print(f"Found {len(blocks)} unique block IDs")
        
        # Now group by time window within each block
        incidents = []
        incident_id = 1
        
        for block_id, block_logs in blocks.items():
            # Sort by timestamp
            block_logs.sort(key=lambda x: x['timestamp'])
            
            # Group by time window
            current_incident = []
            current_start_time = None
            
            for log in block_logs:
                log_time = datetime.fromisoformat(log['timestamp'])
                
                if not current_start_time:
                    # Start new incident
                    current_start_time = log_time
                    current_incident = [log]
                elif log_time - current_start_time <= self.time_window:
                    # Add to current incident
                    current_incident.append(log)
                else:
                    # Save current incident and start new one
                    if current_incident:
                        incidents.append(self._create_incident(incident_id, block_id, current_incident))
                        incident_id += 1
                    current_start_time = log_time
                    current_incident = [log]
            
            # Don't forget last incident
            if current_incident:
                incidents.append(self._create_incident(incident_id, block_id, current_incident))
                incident_id += 1
        
        print(f"Created {len(incidents)} incidents from {len(logs_with_blocks)} logs")
        print(f"Average logs per incident: {len(logs_with_blocks)/len(incidents):.1f}")
        
        return incidents
    
    def _create_incident(self, incident_id: int, block_id: str, logs: List[Dict]) -> Dict:
        """Create incident object from grouped logs."""
        timestamps = [datetime.fromisoformat(log['timestamp']) for log in logs]
        start_time = min(timestamps)
        end_time = max(timestamps)
        
        # Extract severity (highest level in logs)
        levels = [log['level'] for log in logs]
        level_priority = {'FATAL': 4, 'ERROR': 3, 'WARN': 2, 'INFO': 1, 'DEBUG': 0}
        severity = max(levels, key=lambda x: level_priority.get(x, 0))
        
        # Extract affected components
        components = list(set(log['component'] for log in logs))
        
        return {
            "incident_id": incident_id,
            "block_id": block_id,
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "duration_seconds": (end_time - start_time).total_seconds(),
            "num_logs": len(logs),
            "severity": severity,
            "components": components,
            "logs": logs
        }
    
    def save_incidents(self, incidents: List[Dict], output_path: str):
        """Save incidents to JSON file."""
        with open(output_path, 'w') as f:
            json.dump(incidents, f, indent=2)
        print(f"\nSaved {len(incidents)} incidents to {output_path}")
    
    def get_incident_stats(self, incidents: List[Dict]) -> Dict:
        """Get statistics about incidents."""
        if not incidents:
            return {}
        
        num_logs_per_incident = [inc['num_logs'] for inc in incidents]
        durations = [inc['duration_seconds'] for inc in incidents]
        severities = [inc['severity'] for inc in incidents]
        
        return {
            "total_incidents": len(incidents),
            "total_logs": sum(num_logs_per_incident),
            "avg_logs_per_incident": sum(num_logs_per_incident) / len(incidents),
            "min_logs_per_incident": min(num_logs_per_incident),
            "max_logs_per_incident": max(num_logs_per_incident),
            "avg_duration_seconds": sum(durations) / len(durations),
            "severity_distribution": {
                level: severities.count(level) 
                for level in set(severities)
            }
        }


def main():
    """Example usage"""
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python incident_grouper.py <parsed_logs.json> [time_window_minutes]")
        print("Example: python incident_grouper.py ../data/parsed_logs_1000.json 5")
        sys.exit(1)
    
    input_file = sys.argv[1]
    time_window = int(sys.argv[2]) if len(sys.argv) > 2 else 5
    
    # Load parsed logs
    with open(input_file, 'r') as f:
        logs = json.load(f)
    
    print(f"Loaded {len(logs)} parsed logs")
    
    # Group into incidents
    grouper = IncidentGrouper(time_window_minutes=time_window)
    incidents = grouper.group_incidents(logs)
    
    # Save incidents
    output_file = f"../data/incidents_{len(incidents)}.json"
    grouper.save_incidents(incidents, output_file)
    
    # Print stats
    stats = grouper.get_incident_stats(incidents)
    print("\nIncident Statistics:")
    print(json.dumps(stats, indent=2))
    
    # Show sample incident
    if incidents:
        print("\nSample Incident:")
        sample = incidents[0].copy()
        sample['logs'] = f"[{len(sample['logs'])} logs...]"  # Don't print all logs
        print(json.dumps(sample, indent=2))


if __name__ == "__main__":
    main()
