import unittest
import json

# Versión simplificada de process_json_file para propósitos de pruebas
def process_json_file(record):
    try:
        for json_field in ["events_info_json", "schedule_events_info_json",
                           "reschedule_events_info_json", "changes_info_json",
                           "packages_json", "items_json", "extended_info_documents", "statuses"]:
            if record.get(json_field) is not None:
                record[json_field] = json.dumps(record[json_field])
        return record
    except Exception as e:
        return f"Error processing record: {str(e)}"

class TestProcessJsonFile(unittest.TestCase):

    def test_process_json_field_conversion(self):
        
        input_json = {
            "delivery_order_id": "12345",
            "events_info_json": [{"status": 1, "info": {"user_name": "test_user"}}]
        }
        
        processed_record = process_json_file(input_json)
        
        expected_output = json.dumps([{"status": 1, "info": {"user_name": "test_user"}}])
        self.assertEqual(processed_record["events_info_json"], expected_output)

    def test_process_json_file_with_invalid_input(self):

        invalid_json = "esto no es un json válido"
        result = process_json_file(invalid_json)
        
        self.assertIn("Error processing record", result)

if __name__ == '__main__':
    unittest.main()
