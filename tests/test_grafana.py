"""
Consolidated test module for Grafana Dashboards and Configuration.
Contains all tests for dashboard properties, datasource provisioning, and Docker Compose configuration.

Table of Contents:
- Imports and Setup (line ~20)
- Dashboard Refresh Interval Tests (line ~80)
- Dashboard Panel Completeness Tests (line ~150)
- Variable Configuration Tests (line ~350)
- Datasource Provisioning Tests (line ~450)
- Docker Compose Configuration Tests (line ~550)

Requirements: 6.5
"""

# ============================================================================
# IMPORTS AND SETUP
# ============================================================================

import pytest
import json
import yaml
from pathlib import Path
from hypothesis import given, settings, strategies as st


# ============================================================================
# CONSTANTS
# ============================================================================

DASHBOARDS_PATH = Path("grafana/dashboards")
DATASOURCES_PATH = Path("grafana/provisioning/datasources/datasources.yml")
DOCKER_COMPOSE_PATH = Path("docker-compose.yml")

DASHBOARD_REFRESH_INTERVALS = {
    "market-overview.json": "5s",
    "symbol-deep-dive.json": "10s",
    "trading-analytics.json": "1m",
    "system-health.json": "30s",
}

REQUIRED_DATASOURCES = {
    "Redis": {"type": "redis-datasource", "url": "redis:6379"},
    "Prometheus": {"type": "prometheus", "url": "http://prometheus:9090"},
    "FastAPI": {"type": "marcusolsson-json-datasource", "url": "http://crypto-api:8000"},
    "PostgreSQL": {"type": "postgres", "url": "postgres-data:5432"}
}

SYMBOL_VARIABLE_OPTIONS = ["BTC", "ETH", "BNB", "SOL"]
INTERVAL_VARIABLE_OPTIONS = ["1m", "5m", "15m", "1h"]


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def load_dashboard_json(filename: str) -> dict:
    """Load and parse a dashboard JSON file."""
    filepath = DASHBOARDS_PATH / filename
    if not filepath.exists():
        pytest.skip(f"Dashboard file not found: {filepath}")
    
    with open(filepath, "r") as f:
        return json.load(f)


def get_panel_titles(dashboard: dict) -> list:
    """Extract all panel titles from a dashboard."""
    panels = dashboard.get("panels", [])
    return [panel.get("title", "") for panel in panels]


def get_panel_types(dashboard: dict) -> dict:
    """Extract panel types mapped by title."""
    panels = dashboard.get("panels", [])
    return {panel.get("title", ""): panel.get("type", "") for panel in panels}


def load_datasources_yaml():
    """Load and parse the datasources.yml file."""
    if not DATASOURCES_PATH.exists():
        pytest.skip(f"Datasources file not found: {DATASOURCES_PATH}")
    
    with open(DATASOURCES_PATH, "r") as f:
        return yaml.safe_load(f)


def load_docker_compose():
    """Load and parse the docker-compose.yml file."""
    if not DOCKER_COMPOSE_PATH.exists():
        pytest.skip(f"Docker Compose file not found: {DOCKER_COMPOSE_PATH}")
    
    with open(DOCKER_COMPOSE_PATH, "r") as f:
        return yaml.safe_load(f)


def get_grafana_service(config):
    """Extract Grafana service configuration from docker-compose."""
    services = config.get("services", {})
    return services.get("grafana")


# ============================================================================
# DASHBOARD REFRESH INTERVAL TESTS
# ============================================================================

class TestDashboardRefreshIntervals:
    """Property tests for dashboard refresh interval correctness."""

    @given(dashboard_name=st.sampled_from(list(DASHBOARD_REFRESH_INTERVALS.keys())))
    @settings(max_examples=100)
    def test_dashboard_refresh_interval_correctness(self, dashboard_name):
        """
        Feature: grafana-dashboard, Property 2: Dashboard Refresh Interval Correctness
        Validates: Requirements 2.4, 3.5, 4.4, 5.4
        """
        dashboard = load_dashboard_json(dashboard_name)
        expected_refresh = DASHBOARD_REFRESH_INTERVALS[dashboard_name]
        actual_refresh = dashboard.get("refresh")
        
        assert actual_refresh == expected_refresh, \
            f"Dashboard '{dashboard_name}' has wrong refresh interval: " \
            f"expected '{expected_refresh}', got '{actual_refresh}'"
    
    def test_market_overview_refresh_5s(self):
        """Market Overview dashboard SHALL refresh every 5 seconds."""
        dashboard = load_dashboard_json("market-overview.json")
        assert dashboard.get("refresh") == "5s"
    
    def test_symbol_deep_dive_refresh_10s(self):
        """Symbol Deep Dive dashboard SHALL refresh every 10 seconds."""
        dashboard = load_dashboard_json("symbol-deep-dive.json")
        assert dashboard.get("refresh") == "10s"
    
    def test_trading_analytics_refresh_1m(self):
        """Trading Analytics dashboard SHALL refresh every 1 minute."""
        dashboard = load_dashboard_json("trading-analytics.json")
        assert dashboard.get("refresh") == "1m"
    
    def test_system_health_refresh_30s(self):
        """System Health dashboard SHALL refresh every 30 seconds."""
        dashboard = load_dashboard_json("system-health.json")
        assert dashboard.get("refresh") == "30s"


# ============================================================================
# DASHBOARD PANEL COMPLETENESS TESTS
# ============================================================================

class TestDashboardPanelCompleteness:
    """Property tests for dashboard panel completeness."""
    
    def test_market_overview_price_panels(self):
        """Market Overview SHALL display ticker data table with price information."""
        dashboard = load_dashboard_json("market-overview.json")
        panel_titles = get_panel_titles(dashboard)
        panel_types = get_panel_types(dashboard)
        
        # Dashboard uses a table to show all tickers with price data
        assert "All Tickers - Real-time Data" in panel_titles, "Missing All Tickers panel"
        assert panel_types.get("All Tickers - Real-time Data") == "table"
    
    def test_market_overview_volume_panels(self):
        """Market Overview SHALL display top by trades count and quote volume as horizontal bar charts."""
        dashboard = load_dashboard_json("market-overview.json")
        panel_titles = get_panel_titles(dashboard)
        panel_types = get_panel_types(dashboard)
        
        # Dashboard shows top by trades count and quote volume as horizontal bar charts
        assert "Top 5 by Trades Count" in panel_titles, "Missing Top 5 by Trades Count panel"
        assert panel_types.get("Top 5 by Trades Count") == "barchart"
        
        assert "Top 5 by Quote Volume" in panel_titles, "Missing Top 5 by Quote Volume panel"
        assert panel_types.get("Top 5 by Quote Volume") == "barchart"
    
    def test_symbol_deep_dive_price_chart(self):
        """Symbol Deep Dive SHALL display candlestick/timeseries price chart."""
        dashboard = load_dashboard_json("symbol-deep-dive.json")
        panel_titles = get_panel_titles(dashboard)
        panel_types = get_panel_types(dashboard)
        
        price_chart_panels = [t for t in panel_titles if "Price Chart" in t]
        assert len(price_chart_panels) > 0, "Missing Price Chart panel"
        
        for panel_title in price_chart_panels:
            assert panel_types.get(panel_title) == "timeseries"
    
    def test_symbol_deep_dive_no_complex_indicators(self):
        """Symbol Deep Dive SHALL NOT display RSI, MACD, or Technical Indicators panels.
        
        Validates: Requirements 3.3, 3.4, 3.5
        """
        dashboard = load_dashboard_json("symbol-deep-dive.json")
        panel_titles = get_panel_titles(dashboard)
        
        # RSI panel should NOT exist (Requirement 3.3)
        rsi_panels = [t for t in panel_titles if "RSI" in t]
        assert len(rsi_panels) == 0, f"RSI panel should be removed but found: {rsi_panels}"
        
        # MACD panel should NOT exist (Requirement 3.4)
        macd_panels = [t for t in panel_titles if "MACD" in t]
        assert len(macd_panels) == 0, f"MACD panel should be removed but found: {macd_panels}"
        
        # Technical Indicators table should NOT exist (Requirement 3.5)
        indicator_panels = [t for t in panel_titles if "Technical Indicators" in t]
        assert len(indicator_panels) == 0, f"Technical Indicators panel should be removed but found: {indicator_panels}"
    
    def test_trading_analytics_trades_count_chart(self):
        """Trading Analytics SHALL display Trades Count Chart (replacing Volume Heatmap).
        
        Validates: Requirement 2.3
        """
        dashboard = load_dashboard_json("trading-analytics.json")
        panel_titles = get_panel_titles(dashboard)
        panel_types = get_panel_types(dashboard)
        
        # Volume Heatmap should be replaced with Trades Count Chart
        trades_count_panels = [t for t in panel_titles if "giao dịch" in t.lower() or "trades" in t.lower()]
        assert len(trades_count_panels) > 0, "Missing Trades Count Chart panel (Số giao dịch theo thời gian)"
        
        # Verify it's a bar chart type
        for panel_title in trades_count_panels:
            panel_type = panel_types.get(panel_title)
            assert panel_type == "barchart", f"Trades Count Chart should be barchart type, got: {panel_type}"
    
    def test_system_health_pipeline_panels(self):
        """System Health SHALL display Messages/sec graph."""
        dashboard = load_dashboard_json("system-health.json")
        panel_titles = get_panel_titles(dashboard)
        panel_types = get_panel_types(dashboard)
        
        assert "Messages/sec" in panel_titles
        assert panel_types.get("Messages/sec") == "timeseries"
    
    def test_system_health_status_panels(self):
        """System Health SHALL display status panels for Kafka, Redis, API."""
        dashboard = load_dashboard_json("system-health.json")
        panel_titles = get_panel_titles(dashboard)
        panel_types = get_panel_types(dashboard)
        
        required_panels = ["Kafka Status", "Redis Status", "API Status"]
        for panel_name in required_panels:
            assert panel_name in panel_titles, f"Missing status panel: {panel_name}"
            assert panel_types.get(panel_name) == "stat"
    
    @given(dashboard_name=st.sampled_from(list(DASHBOARD_REFRESH_INTERVALS.keys())))
    @settings(max_examples=100)
    def test_dashboard_has_panels(self, dashboard_name):
        """For any dashboard JSON file, the panels array SHALL contain at least one panel."""
        dashboard = load_dashboard_json(dashboard_name)
        panels = dashboard.get("panels", [])
        
        assert len(panels) > 0, f"Dashboard '{dashboard_name}' has no panels"


# ============================================================================
# VARIABLE CONFIGURATION TESTS
# ============================================================================

class TestVariableConfiguration:
    """Property tests for dashboard variable configuration correctness."""
    
    def test_symbol_deep_dive_has_symbol_variable(self):
        """Symbol Deep Dive SHALL have symbol variable."""
        dashboard = load_dashboard_json("symbol-deep-dive.json")
        templating = dashboard.get("templating", {})
        variables = templating.get("list", [])
        
        symbol_var = next((v for v in variables if v.get("name") == "symbol"), None)
        
        assert symbol_var is not None, "Missing 'symbol' variable"
        assert symbol_var.get("type") == "custom"
    
    def test_symbol_deep_dive_has_interval_variable(self):
        """Symbol Deep Dive SHALL have interval variable."""
        dashboard = load_dashboard_json("symbol-deep-dive.json")
        templating = dashboard.get("templating", {})
        variables = templating.get("list", [])
        
        interval_var = next((v for v in variables if v.get("name") == "interval"), None)
        
        assert interval_var is not None, "Missing 'interval' variable"
        assert interval_var.get("type") == "custom"
    
    @given(symbol=st.sampled_from(SYMBOL_VARIABLE_OPTIONS))
    @settings(max_examples=100)
    def test_symbol_variable_contains_required_options(self, symbol):
        """For any required symbol, the symbol variable SHALL contain that option."""
        dashboard = load_dashboard_json("symbol-deep-dive.json")
        templating = dashboard.get("templating", {})
        variables = templating.get("list", [])
        
        symbol_var = next((v for v in variables if v.get("name") == "symbol"), None)
        assert symbol_var is not None
        
        options = symbol_var.get("options", [])
        option_values = [opt.get("value") for opt in options]
        
        assert symbol in option_values, f"Symbol variable missing option: {symbol}"
    
    @given(interval=st.sampled_from(INTERVAL_VARIABLE_OPTIONS))
    @settings(max_examples=100)
    def test_interval_variable_contains_required_options(self, interval):
        """For any required interval, the interval variable SHALL contain that option."""
        dashboard = load_dashboard_json("symbol-deep-dive.json")
        templating = dashboard.get("templating", {})
        variables = templating.get("list", [])
        
        interval_var = next((v for v in variables if v.get("name") == "interval"), None)
        assert interval_var is not None
        
        options = interval_var.get("options", [])
        option_values = [opt.get("value") for opt in options]
        
        assert interval in option_values, f"Interval variable missing option: {interval}"


# ============================================================================
# DATASOURCE PROVISIONING TESTS
# ============================================================================

class TestDataSourceProvisioning:
    """Property tests for data source provisioning completeness."""
    
    def test_datasources_file_exists(self):
        """The datasources.yml file must exist."""
        assert DATASOURCES_PATH.exists()
    
    def test_datasources_yaml_valid(self):
        """The datasources.yml file must be valid YAML."""
        config = load_datasources_yaml()
        assert config is not None
        assert "datasources" in config
    
    def test_datasources_count(self):
        """Datasources SHALL contain exactly 4 data sources."""
        config = load_datasources_yaml()
        datasources = config.get("datasources", [])
        assert len(datasources) == 4
    
    def test_all_required_datasources_present(self):
        """Datasources SHALL contain Redis, Prometheus, FastAPI, PostgreSQL."""
        config = load_datasources_yaml()
        datasources = config.get("datasources", [])
        
        datasource_names = {ds.get("name") for ds in datasources}
        required_names = set(REQUIRED_DATASOURCES.keys())
        
        assert required_names.issubset(datasource_names)
    
    @given(datasource_name=st.sampled_from(list(REQUIRED_DATASOURCES.keys())))
    @settings(max_examples=100)
    def test_datasource_configuration_correctness(self, datasource_name):
        """For any required data source, the configuration SHALL have correct type and URL."""
        config = load_datasources_yaml()
        datasources = config.get("datasources", [])
        
        datasource = next((ds for ds in datasources if ds.get("name") == datasource_name), None)
        assert datasource is not None
        
        expected = REQUIRED_DATASOURCES[datasource_name]
        assert datasource.get("type") == expected["type"]
        assert datasource.get("url") == expected["url"]


# ============================================================================
# DOCKER COMPOSE CONFIGURATION TESTS
# ============================================================================

class TestDockerComposeGrafanaConfiguration:
    """Property tests for Docker Compose Grafana service configuration."""
    
    def test_docker_compose_file_exists(self):
        """The docker-compose.yml file must exist."""
        assert DOCKER_COMPOSE_PATH.exists()
    
    def test_docker_compose_yaml_valid(self):
        """The docker-compose.yml file must be valid YAML."""
        config = load_docker_compose()
        assert config is not None
        assert "services" in config
    
    def test_grafana_service_exists(self):
        """Grafana service SHALL exist in docker-compose.yml."""
        config = load_docker_compose()
        grafana = get_grafana_service(config)
        
        assert grafana is not None
    
    def test_grafana_image_configured(self):
        """Grafana service SHALL use grafana/grafana image."""
        config = load_docker_compose()
        grafana = get_grafana_service(config)
        
        assert grafana is not None
        image = grafana.get("image", "")
        assert "grafana/grafana" in image
    
    def test_grafana_port_3000_exposed(self):
        """Grafana SHALL expose web UI on port 3000."""
        config = load_docker_compose()
        grafana = get_grafana_service(config)
        
        assert grafana is not None
        ports = grafana.get("ports", [])
        
        port_3000_mapped = any("3000" in str(port) for port in ports)
        assert port_3000_mapped
    
    def test_grafana_provisioning_volume_mounted(self):
        """Grafana SHALL have provisioning volume mounted."""
        config = load_docker_compose()
        grafana = get_grafana_service(config)
        
        assert grafana is not None
        volumes = grafana.get("volumes", [])
        
        provisioning_mounted = any(
            "/etc/grafana/provisioning" in str(vol) 
            for vol in volumes
        )
        assert provisioning_mounted
    
    def test_grafana_dashboards_volume_mounted(self):
        """Grafana SHALL have dashboards volume mounted."""
        config = load_docker_compose()
        grafana = get_grafana_service(config)
        
        assert grafana is not None
        volumes = grafana.get("volumes", [])
        
        dashboards_mounted = any(
            "/var/lib/grafana/dashboards" in str(vol) 
            for vol in volumes
        )
        assert dashboards_mounted
    
    def test_grafana_network_configured(self):
        """Grafana SHALL be connected to streaming-network."""
        config = load_docker_compose()
        grafana = get_grafana_service(config)
        
        assert grafana is not None
        networks = grafana.get("networks", [])
        
        assert "streaming-network" in networks
    
    def test_grafana_depends_on_redis(self):
        """Grafana SHALL depend on Redis service."""
        config = load_docker_compose()
        grafana = get_grafana_service(config)
        
        assert grafana is not None
        depends_on = grafana.get("depends_on", {})
        
        if isinstance(depends_on, list):
            has_redis = "redis" in depends_on
        else:
            has_redis = "redis" in depends_on
        
        assert has_redis
    
    @given(volume_type=st.sampled_from(["provisioning", "dashboards"]))
    @settings(max_examples=100)
    def test_required_volumes_mounted(self, volume_type):
        """For any required volume type, Grafana SHALL have it properly mounted."""
        config = load_docker_compose()
        grafana = get_grafana_service(config)
        
        assert grafana is not None
        volumes = grafana.get("volumes", [])
        
        expected_paths = {
            "provisioning": "/etc/grafana/provisioning",
            "dashboards": "/var/lib/grafana/dashboards"
        }
        
        expected_path = expected_paths[volume_type]
        volume_mounted = any(expected_path in str(vol) for vol in volumes)
        
        assert volume_mounted
    
    def test_grafana_plugins_configured(self):
        """Grafana SHALL have required plugins configured."""
        config = load_docker_compose()
        grafana = get_grafana_service(config)
        
        assert grafana is not None
        environment = grafana.get("environment", {})
        
        plugins = environment.get("GF_INSTALL_PLUGINS", "")
        
        assert "redis-datasource" in plugins
        assert "marcusolsson-json-datasource" in plugins
