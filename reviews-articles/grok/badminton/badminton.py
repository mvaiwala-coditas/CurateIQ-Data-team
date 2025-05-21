import os
import time
import json
import requests
import pandas as pd
from openai import OpenAI
from openai import OpenAIError

def search_grok_api(search_prompt: str, api_key: str, model: str = "grok-3", max_tokens: int = 3000, max_retries: int = 3, log_file: str = "grok_api_log.json") -> dict:
    """
    Send a search prompt to the Grok API and return the response, with follow-up for tool calls.
    
    Args:
        search_prompt (str): The search query to send to the API.
        api_key (str): Your xAI API key.
        model (str): The Grok model to use (default: grok-3).
        max_tokens (int): Maximum tokens for the response (default: 3000).
        max_retries (int): Maximum retry attempts for failed API calls (default: 3).
        log_file (str): File to log API responses for debugging (default: grok_api_log.json).
    
    Returns:
        dict: The API response containing search results or an error message.
    """
    retry_count = 0
    base_delay = 1  # 1 second base delay for retries
    current_prompt = search_prompt
    
    client = OpenAI(
        api_key=api_key,
        base_url="https://api.x.ai/v1"
    )

    while retry_count < max_retries:
        try:
            # Make the initial API call
            response = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "user", "content": f"Search for {current_prompt}"}
                ],
                tools=[
                    {
                        "type": "function",
                        "function": {
                            "name": "web_search",
                            "description": "Perform a web search to retrieve real-time information",
                            "parameters": {
                                "type": "object",
                                "properties": {
                                    "query": {"type": "string", "description": "The search query"}
                                },
                                "required": ["query"]
                            }
                        }
                    }
                ],
                tool_choice={"type": "function", "function": {"name": "web_search"}},
                max_tokens=max_tokens,
                temperature=0.7,
                timeout=90  
            )

            
            with open(log_file, 'a', encoding='utf-8') as f:
                json.dump(response.to_dict(), f, indent=2)
                f.write('\n')

            # Extract content
            content = response.choices[0].message.content or ""
            tool_output = ""

            
            if response.choices[0].message.tool_calls:
                for tool_call in response.choices[0].message.tool_calls:
                    if tool_call.function.name == "web_search":
                        try:
                            tool_args = json.loads(tool_call.function.arguments)
                            tool_output += f"\nWeb search executed for query: {tool_args.get('query', 'unknown')}"

                            # Follow-up API call to retrieve tool results
                            follow_up_response = client.chat.completions.create(
                                model=model,
                                messages=[
                                    {"role": "user", "content": f"Provide the results for the web search: {tool_args.get('query', 'unknown')}"},
                                    {
                                        "role": "assistant",
                                        "content": content,
                                        "tool_calls": response.choices[0].message.tool_calls
                                    }
                                ],
                                max_tokens=max_tokens,
                                temperature=0.7,
                                timeout=90
                            )

                            # Log follow-up response
                            with open(log_file, 'a', encoding='utf-8') as f:
                                json.dump(follow_up_response.to_dict(), f, indent=2)
                                f.write('\n')

                            # Update content with follow-up results
                            follow_up_content = follow_up_response.choices[0].message.content
                            if follow_up_content:
                                content = follow_up_content
                        except (json.JSONDecodeError, OpenAIError) as e:
                            tool_output += f"\nFollow-up failed or arguments could not be parsed: {str(e)}"

            # Check for incomplete or empty response
            if (not content or "Initiating a web search" in content or len(content.strip()) < 100) and not tool_output.lower().find("follow-up") != -1:
                retry_count += 1
                if retry_count < max_retries:
                    delay = base_delay * (2 ** retry_count)
                    print(f"Incomplete or empty response. Retrying in {delay} seconds...")
                    time.sleep(delay)
                    current_prompt = f"{search_prompt} (retry {retry_count})"
                    continue
                return {
                    "status": "error",
                    "content": f"Error: Incomplete response after {max_retries} retries. Raw tool output: {tool_output}"
                }

            # Combine content and tool output
            final_content = content + tool_output

            # Extract response
            result = {
                "status": "success",
                "content": final_content,
                "model": response.model,
                "created": response.created
            }
            return result

        except OpenAIError as e:
            retry_count += 1
            if retry_count < max_retries:
                delay = base_delay * (2 ** retry_count)
                print(f"API error: {str(e)}. RetryingNSW in {delay} seconds...")
                time.sleep(delay)
                current_prompt = f"{search_prompt} (retry {retry_count})"
            else:
                return {
                    "status": "error",
                    "content": f"Error after {max_retries} retries: {str(e)}"
                }
        except requests.exceptions.Timeout:
            retry_count += 1
            if retry_count < max_retries:
                delay = base_delay * (2 ** retry_count)
                print(f"Request timed out. Retrying in {delay} seconds...")
                time.sleep(delay)
                current_prompt = f"{search_prompt} (retry {retry_count})"
            else:
                return {
                    "status": "error",
                    "content": f"Error after {max_retries} retries: Request timed out"
                }
        except Exception as e:
            return {
                "status": "error",
                "content": f"Unexpected error: {str(e)}"
            }

def main():
    default_api_key = os.getenv("api_key")
    api_key = os.getenv("api_key")
    if not api_key:
        api_key = default_api_key
        print("Warning: XAI_API_KEY environment variable not set. Using default API key. Replace 'YOUR_API_KEY_HERE' in the script with your actual key.")

    if not api_key or api_key == "YOUR_API_KEY_HERE":
        print("Error: No valid API key provided. Set XAI_API_KEY environment variable or update the default_api_key in the script.")
        return

    # Read the Excel file
    try:
        df = pd.read_excel('badminton.xlsx', engine='openpyxl')
        print("Successfully read Excel file")
        print("Columns found:", df.columns.tolist())
    except Exception as e:
        print(f"Error reading Excel file: {str(e)}")
        return

    # Process each row
    for index, row in df.iterrows():
        try:
            # Create search prompt from row data
            search_prompt = f"""
            Please search for and summarize the articles and reviews you can find on 
            URL : {row['URL']}
            Name : {row['Name']}
            Brand : {row['Brand']}
            
            using latest information you can find (e.g., tennis-warehouse.com, tennisexpress.com, badmintonwarehouse.com, badmintonavenue.com, badmintondirect.com, btracketsports.com, badmintonalley.com, dickssportinggoods.com, badmintoncentral.com, racketsportsworld.com and other trusted sources) and attched their source page link as well.
            """

            # Call the API
            result = search_grok_api(search_prompt, api_key)

            # Store the result in the review column
            if result["status"] == "success":
                df.at[index, 'review'] = result["content"]
                print(f"\nProcessed row {index + 1}:")
                print(f"Model: {result['model']}")
                print(f"Created: {result['created']}")
            else:
                print(f"\nError processing row {index + 1}: {result['content']}")
                df.at[index, 'review'] = f"Error: {result['content']}"

            # Save after each row to prevent data loss
            df.to_excel('badminton.xlsx', index=False, engine='openpyxl')
            print(f"Saved results for row {index + 1}")
        except Exception as e:
            print(f"Error processing row {index + 1}: {str(e)}")
            continue

if __name__ == "__main__":
    main()