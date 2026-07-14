from flask import Flask

from pdc.config import SECRET_KEY, DO_SPACES_CDN


def create_app():
    app = Flask(__name__)
    app.config["SECRET_KEY"] = SECRET_KEY

    # Run schema DDL once at worker boot rather than per request. A transient
    # DB outage must not crash the worker — ensure_schema() retries on the
    # next get_db() call.
    try:
        from pdc.db import ensure_schema
        ensure_schema()
    except Exception:
        app.logger.exception("Schema init failed at boot; will retry on first request")

    # Liveness probe for Railway. Deliberately DB-free: if the database has
    # a blip we want 500s on content pages, not a platform restart loop.
    @app.route("/healthz")
    def healthz():
        return {"status": "ok"}, 200

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
