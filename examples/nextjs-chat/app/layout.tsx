import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "FleetLM Chat Demo",
  description: "Next.js chat example with FleetLM conversation logs."
};

export default function RootLayout({
  children
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
