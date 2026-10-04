import type { Metadata } from "next";
import { Geist, Geist_Mono } from "next/font/google";
import "./globals.css";
import { QueryProvider } from "@/providers/query-provider";
import { RoleProvider } from "@/providers/role-provider";
import { SSEProvider } from "@/providers/sse-provider";
import { Shell } from "@/components/layout/shell";
import { Toaster } from "@/components/ui/sonner";

const geistSans = Geist({
  variable: "--font-geist-sans",
  subsets: ["latin"],
});

const geistMono = Geist_Mono({
  variable: "--font-geist-mono",
  subsets: ["latin"],
});

export const metadata: Metadata = {
  title: "StreamRune E-Commerce Demo",
  description: "Event-sourced e-commerce demo powered by StreamRune",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html
      lang="en"
      className={`${geistSans.variable} ${geistMono.variable} h-full antialiased dark`}
    >
      <body className="min-h-full flex flex-col bg-background text-foreground">
        <QueryProvider>
          <RoleProvider>
            <SSEProvider>
              <Shell>{children}</Shell>
            </SSEProvider>
          </RoleProvider>
        </QueryProvider>
        <Toaster />
      </body>
    </html>
  );
}
