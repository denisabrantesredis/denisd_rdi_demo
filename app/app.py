import os
import argparse
from redis import Redis
from datetime import timedelta
from configparser import ConfigParser
from flask import Flask, session

def parse_args():
    parser = argparse.ArgumentParser(description="Insert Records")
    parser.add_argument(
        "--hostname",
        type=str,
        help="Hostname",
        default="localhost",
        required=False
    )
    parser.add_argument(
        "--port",
        type=int,
        help="Starting Port",
        default=5000,
        required=False
    )
    parser.add_argument(
        "--debug",
        type=str,
        help="Debug Mode",
        default="false",
        required=False
    )
    return parser.parse_args()


app = Flask( __name__ , instance_relative_config=False)

app.config["SECRET_KEY"] = "secretkey123"
app.secret_key = "secretkey123"

with app.app_context():
        # Import parts of our application
        from home import home_bp
        app.register_blueprint(home_bp)

if __name__ == '__main__':
    args = parse_args()
    hostname = args.hostname
    port = args.port
    debug = (args.debug=="true")
    app.run(debug=True, host=hostname, port=port)