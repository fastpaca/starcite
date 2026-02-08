// @ts-check

const siteUrl = process.env.DOCUSAURUS_SITE_URL || "https://fleetlm.com/docs";
// Docusaurus `url` must be the site origin (no path); serve under `/docs/` via `baseUrl`.
const baseUrl = process.env.DOCUSAURUS_BASE_URL || "/docs/";

const canonicalSiteUrl = siteUrl.replace(/\/docs\/?$/, "");

const config = {
  title: "FleetLM Docs",
  tagline: "AI sessions that don't break",
  url: canonicalSiteUrl,
  baseUrl,
  onBrokenLinks: "throw",
  onBrokenMarkdownLinks: "warn",
  favicon: "https://docusaurus.io/favicon.ico",
  organizationName: "fastpaca",
  projectName: "fleetlm",
  trailingSlash: false,
  i18n: {
    defaultLocale: "en",
    locales: ["en"]
  },
  markdown: {
    mermaid: true,
  },
  themeConfig: {
    announcementBar: {
      id: "wip-banner",
      content:
        "<strong>Early access:</strong> FleetLM is an active work in progress. Expect rapid changes and review carefully before running in production.",
      backgroundColor: "#f97316",
      textColor: "#1f2937",
      isCloseable: false
    }
  },
  themes: ["@docusaurus/theme-mermaid"],
  presets: [
    [
      "@docusaurus/preset-classic",
      {
        docs: {
          path: "../docs",
          routeBasePath: "/",
          sidebarPath: require.resolve("./sidebars.js"),
          editUrl: "https://github.com/fastpaca/fleetlm/tree/main/docs/"
        },
        blog: false,
        theme: {
          customCss: require.resolve("./src/css/custom.css")
        }
      }
    ]
  ]
};

module.exports = config;
