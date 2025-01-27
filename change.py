from pymongo import MongoClient
import time
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ChangeStreamTester:
    def __init__(self, connection_string="mongodb://localhost:27017/"):
        self.client = MongoClient(connection_string)
        self.db = self.client.test_db
        self.collection = self.db.test_collection
        
    def setup(self):
        """Setup test collection and indexes"""
        # Clear existing data
        self.collection.drop()
        
        # Create an index to enable change streams
        self.collection.create_index("_id")
        logger.info("Test collection setup complete")
        
    def test_resume_token_persistence(self, iterations=5):
        """Test if change streams can be resumed after interruption"""
        # Start change stream and get initial resume token
        change_stream = self.collection.watch()
        initial_resume_token = change_stream.resume_token
        logger.info(f"Initial resume token: {initial_resume_token}")
        
        # Insert some documents
        for i in range(iterations):
            self.collection.insert_one({"test": i, "timestamp": datetime.now()})
            logger.info(f"Inserted document {i}")
            
        # Get events and store resume token
        events = []
        for change in change_stream:
            events.append(change)
            if len(events) >= iterations:
                break
                
        last_resume_token = change_stream.resume_token
        change_stream.close()
        
        logger.info(f"Collected {len(events)} events")
        logger.info(f"Last resume token: {last_resume_token}")
        
        # Test resuming from last token
        resumed_stream = self.collection.watch(resume_after=last_resume_token)
        logger.info("Successfully resumed change stream")
        
        # Insert more documents to verify resumed stream
        new_events = []
        for i in range(iterations):
            self.collection.insert_one({"test": f"resumed_{i}", "timestamp": datetime.now()})
            
        for change in resumed_stream:
            new_events.append(change)
            if len(new_events) >= iterations:
                break
                
        resumed_stream.close()
        
        return {
            "initial_events": len(events),
            "resumed_events": len(new_events),
            "success": len(new_events) == iterations
        }
        
    def test_stream_durability(self, disconnect_duration=5):
        """Test if change streams survive temporary disconnections"""
        change_stream = self.collection.watch()
        
        # Insert initial document
        self.collection.insert_one({"phase": "pre-disconnect", "timestamp": datetime.now()})
        
        # Get first event
        pre_disconnect_events = []
        for change in change_stream:
            pre_disconnect_events.append(change)
            break
            
        resume_token = change_stream.resume_token
        change_stream.close()
        
        logger.info(f"Simulating disconnect for {disconnect_duration} seconds")
        time.sleep(disconnect_duration)
        
        # Try to resume after "disconnection"
        try:
            resumed_stream = self.collection.watch(resume_after=resume_token)
            self.collection.insert_one({"phase": "post-disconnect", "timestamp": datetime.now()})
            
            post_disconnect_events = []
            for change in resumed_stream:
                post_disconnect_events.append(change)
                break
                
            resumed_stream.close()
            
            return {
                "pre_disconnect_events": len(pre_disconnect_events),
                "post_disconnect_events": len(post_disconnect_events),
                "success": len(post_disconnect_events) > 0
            }
            
        except Exception as e:
            logger.error(f"Failed to resume after disconnect: {e}")
            return {
                "pre_disconnect_events": len(pre_disconnect_events),
                "post_disconnect_events": 0,
                "success": False,
                "error": str(e)
            }
            
    def run_all_tests(self):
        """Run all change stream tests"""
        self.setup()
        
        results = {
            "resume_token_test": self.test_resume_token_persistence(),
            "durability_test": self.test_stream_durability()
        }
        
        return results

if __name__ == "__main__":
    tester = ChangeStreamTester()
    results = tester.run_all_tests()
    logger.info("Test Results:")
    logger.info(results)
