#!/usr/bin/env python3
"""
Quick Test Script
Tests parser and grouper with sample HDFS logs
"""

import sys
import os

# Sample HDFS log lines (from real HDFS dataset)
SAMPLE_LOGS = """081109 203518 143 INFO dfs.DataNode$DataXceiver: Receiving block blk_-1608999687919862906 src: /10.250.19.102:54106 dest: /10.250.19.102:50010
081109 203518 148 INFO dfs.DataNode$DataXceiver: Receiving block blk_-1608999687919862906 src: /10.250.19.102:54108 dest: /10.250.19.102:50010
081109 203519 150 INFO dfs.DataNode$PacketResponder: PacketResponder 1 for block blk_-1608999687919862906 terminating
081109 203520 148 INFO dfs.DataNode$DataXceiver: Receiving block blk_3540344223099725891 src: /10.250.10.6:49890 dest: /10.250.10.6:50010
081109 203521 148 INFO dfs.DataNode$PacketResponder: PacketResponder 2 for block blk_3540344223099725891 terminating
081109 203522 143 INFO dfs.DataNode$DataXceiver: Receiving block blk_-1608999687919862906 src: /10.250.19.102:54110 dest: /10.250.19.102:50010
081109 203522 148 WARN dfs.DataNode: DataNode lost connection to block blk_-1608999687919862906
081109 203523 148 ERROR dfs.DataNode: DataNode error processing block blk_-1608999687919862906
081109 203524 150 INFO dfs.DataNode$PacketResponder: PacketResponder 3 for block blk_-1608999687919862906 terminating
081109 203525 151 INFO dfs.FSNamesystem: BLOCK* NameSystem.allocateBlock: /user/root/randtxt4/_temporary/_task_200811092030_0001_m_000005_0/part-00005. blk_-8611209185556947820
081109 203526 143 INFO dfs.DataNode$DataXceiver: Receiving block blk_5080623248220211130 src: /10.251.73.220:54338 dest: /10.251.73.220:50010
081109 203527 143 INFO dfs.DataNode$PacketResponder: PacketResponder 1 for block blk_5080623248220211130 terminating
081109 203528 148 INFO dfs.DataNode$DataXceiver: Receiving block blk_3540344223099725891 src: /10.250.10.6:49892 dest: /10.250.10.6:50010
081109 203529 148 INFO dfs.DataNode$PacketResponder: PacketResponder 2 for block blk_3540344223099725891 terminating
081109 203530 150 INFO dfs.DataNode$DataXceiver: Receiving block blk_-1608999687919862906 src: /10.250.19.102:54112 dest: /10.250.19.102:50010"""


def test_parser():
    """Test HDFS parser with sample data"""
    print("=" * 60)
    print("Testing HDFS Parser")
    print("=" * 60)
    
    # Import parser
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))
    from hdfs_parser import HDFSLogParser
    
    # Create sample file
    os.makedirs('data', exist_ok=True)
    sample_file = 'data/sample_hdfs.log'
    with open(sample_file, 'w') as f:
        f.write(SAMPLE_LOGS)
    
    print(f"\nCreated sample file: {sample_file} with {len(SAMPLE_LOGS.split(chr(10)))} lines\n")
    
    # Parse
    parser = HDFSLogParser()
    logs = parser.parse_file(sample_file)
    
    # Show results
    print(f"\n✓ Successfully parsed {len(logs)} logs")
    print("\nSample parsed log:")
    print(f"  Timestamp: {logs[0]['timestamp']}")
    print(f"  Level: {logs[0]['level']}")
    print(f"  Component: {logs[0]['component']}")
    print(f"  Block ID: {logs[0]['block_id']}")
    print(f"  Message: {logs[0]['message'][:60]}...")
    
    return logs


def test_grouper(logs):
    """Test incident grouper"""
    print("\n" + "=" * 60)
    print("Testing Incident Grouper")
    print("=" * 60)
    
    # Import grouper
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))
    from incident_grouper import IncidentGrouper
    
    # Group
    grouper = IncidentGrouper(time_window_minutes=5)
    incidents = grouper.group_incidents(logs)
    
    # Show results
    print(f"\n✓ Created {len(incidents)} incidents")
    
    stats = grouper.get_incident_stats(incidents)
    print("\nIncident Statistics:")
    print(f"  Average logs per incident: {stats['avg_logs_per_incident']:.1f}")
    print(f"  Incident size range: {stats['min_logs_per_incident']} - {stats['max_logs_per_incident']}")
    print(f"  Severity distribution: {stats['severity_distribution']}")
    
    print("\nSample Incident:")
    sample = incidents[0]
    print(f"  ID: {sample['incident_id']}")
    print(f"  Block: {sample['block_id']}")
    print(f"  Logs: {sample['num_logs']}")
    print(f"  Severity: {sample['severity']}")
    print(f"  Duration: {sample['duration_seconds']:.1f}s")
    
    return incidents


def main():
    print("\n" + "=" * 60)
    print("EC528 HDFS Log Analysis - Quick Test")
    print("=" * 60 + "\n")
    
    try:
        # Test parser
        logs = test_parser()
        
        # Test grouper
        incidents = test_grouper(logs)
        
        print("\n" + "=" * 60)
        print("✓ All tests passed!")
        print("=" * 60)
        print("\nNext steps:")
        print("1. Download full HDFS dataset from:")
        print("   https://zenodo.org/record/3227177/files/HDFS_1.tar.gz")
        print("2. Run: python src/hdfs_parser.py data/HDFS.log 1000")
        print("3. Run: python src/incident_grouper.py data/parsed_logs_1000.json")
        print("4. Open: notebooks/01_data_exploration.ipynb")
        print()
        
    except Exception as e:
        print(f"\n✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
