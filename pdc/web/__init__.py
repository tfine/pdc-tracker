from flask import Flask

from pdc.config import SECRET_KEY, DO_SPACES_CDN


def create_app():
    app = Flask(__name__)
    app.config["SECRET_KEY"] = SECRET_KEY

    # Make CDN URL available in all templates
    @app.context_processor
    def inject_globals():
        return {"CDN": DO_SPACES_CDN}

    # Register blueprints
    from pdc.web.routes.home import bp as home_bp
    from pdc.web.routes.projects import bp as projects_bp
    from pdc.web.routes.meetings import bp as meetings_bp
    from pdc.web.routes.transcripts import bp as transcripts_bp
    from pdc.web.routes.alerts import bp as alerts_bp
    from pdc.web.routes.about import bp as about_bp

    app.register_blueprint(home_bp)
    app.register_blueprint(projects_bp)
    app.register_blueprint(meetings_bp)
    app.register_blueprint(transcripts_bp)
    app.register_blueprint(alerts_bp)
    app.register_blueprint(about_bp)

    # Custom error pages
    @app.errorhandler(404)
    def not_found(e):
        from flask import render_template
        return render_template("404.html"), 404

    @app.errorhandler(500)
    def server_error(e):
        from flask import render_template
        return render_template("500.html"), 500

    return app
