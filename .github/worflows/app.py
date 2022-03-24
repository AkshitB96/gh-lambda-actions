return {
        "statusCode": 200,
        "body": json.dumps({
            "abc": ip.status_code,
            "message": os.environ.get('MyParam')
        }),
    }