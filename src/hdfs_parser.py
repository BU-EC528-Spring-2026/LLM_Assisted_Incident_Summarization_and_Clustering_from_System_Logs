"""
HDFS Log Parser
Parses HDFS logs from LogHub format into structured JSON
"""

import re
import json
from datetime import datetime
from typing import Dict, List, Optional
from pathlib import Path


class HDFSLogParser:
    """
    Parser for HDFS logs from LogHub dataset.
    
    Expected log format:
    DATE TIME THREAD_ID LEVEL COMPONENT: MESSAGE
    Example: 081109 203518 143 INFO dfs.DataNode$DataXceiver: Receiving block blk_-1608999687919862906
    """
    
    def __init__(self):
        # Regex pattern for HDFS logs
        # Captures: date, time, thread_id, level, component, message
        self.pattern = re.compile(
            r'^(\d{6})\s+(\d{6})\s+(\d+)\s+(\w+)\s+([\w.$]+):\s+(.+)$'
        )
        
        # Block ID pattern (appears in messages)
        self.block_pattern = re.compile(r'blk_-?\d+')
        
    def parse_line(self, line: str, line_number: int) -> Optional[Dict]:
        """
        Parse a single log line into structured format.
        
        Args:
            line: Raw log line
            line_number: Line number in file (for debugging)
            
        Returns:
            Dictionary with parsed fields or None if parse fails
        """
        line = line.strip()
        if not line:
            return None
            
        match = self.pattern.match(line)
        if not match:
            # Log parse failure but continue
            print(f"Warning: Failed to parse line {line_number}: {line[:100]}")
            return None
            
        date_str, time_str, thread_id, level, component, message = match.groups()
        
        # Parse timestamp (YYMMDD HHMMSS -> datetime)
        try:
            timestamp = datetime.strptime(f"{date_str} {time_str}", "%y%m%d %H%M%S")
            timestamp_str = timestamp.isoformat()
        except ValueError:
            timestamp_str = f"20{date_str[:2]}-{date_str[2:4]}-{date_str[4:6]}T{time_str[:2]}:{time_str[2:4]}:{time_str[4:6]}"
        
        # Extract block ID if present
        block_match = self.block_pattern.search(message)
        block_id = block_match.group(0) if block_match else None
        
        return {
            "line_number": line_number,
            "timestamp": timestamp_str,
            "thread_id": thread_id,
            "level": level,
            "component": component,
            "message": message,
            "block_id": block_id,
            "raw_line": line
        }
    
    def parse_file(self, filepath: str, max_lines: Optional[int] = None) -> List[Dict]:
        """
        Parse entire log file.
        
        Args:
            filepath: Path to HDFS log file
            max_lines: Maximum lines to parse (None = all)
            
        Returns:
            List of parsed log entries
        """
        parsed_logs = []
        failed_count = 0
        
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            for line_num, line in enumerate(f, 1):
                if max_lines and line_num > max_lines:
                    break
                    
                parsed = self.parse_line(line, line_num)
                if parsed:
                    parsed_logs.append(parsed)
                else:
                    failed_count += 1
        
        print(f"\nParsing Summary:")
        print(f"  Total lines processed: {line_num}")
        print(f"  Successfully parsed: {len(parsed_logs)}")
        print(f"  Failed to parse: {failed_count}")
        print(f"  Success rate: {len(parsed_logs)/line_num*100:.1f}%")
        
        return parsed_logs
    
    def save_to_json(self, logs: List[Dict], output_path: str):
        """Save parsed logs to JSON file."""
        with open(output_path, 'w') as f:
            json.dump(logs, f, indent=2)
        print(f"\nSaved {len(logs)} logs to {output_path}")


def main():
    """Example usage"""
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python hdfs_parser.py <input_log_file> [max_lines]")
        print("Example: python hdfs_parser.py ../data/HDFS.log 1000")
        sys.exit(1)
    
    input_file = sys.argv[1]
    max_lines = int(sys.argv[2]) if len(sys.argv) > 2 else None
    
    # Parse logs
    parser = HDFSLogParser()
    parsed_logs = parser.parse_file(input_file, max_lines)
    
    # Save to JSON
    output_file = f"../data/parsed_logs_{len(parsed_logs)}.json"
    parser.save_to_json(parsed_logs, output_file)
    
    # Show sample
    if parsed_logs:
        print("\nSample parsed log:")
        print(json.dumps(parsed_logs[0], indent=2))


if __name__ == "__main__":
    main()
