"use client";

import type React from "react";
import Link from "next/link";
import { usePathname } from "next/navigation";
import {
  LayoutDashboard,
  ShoppingCart,
  Package,
  Users,
  Activity,
  ListTree,
  ClipboardList,
  Settings,
  Zap,
} from "lucide-react";
import { cn } from "@/lib/utils";
import { useSSE } from "@/providers/sse-provider";
import { RoleSwitcher } from "./role-switcher";

interface NavItem {
  label: string;
  href: string;
  icon: React.ComponentType<React.SVGProps<SVGSVGElement>>;
  showSSE?: boolean;
}

const NAV_ITEMS: NavItem[] = [
  { label: "Dashboard", href: "/", icon: LayoutDashboard },
  { label: "Orders", href: "/orders", icon: ShoppingCart },
  { label: "Products", href: "/products", icon: Package },
  { label: "Customers", href: "/customers", icon: Users },
  { label: "Fulfillment", href: "/fulfillment", icon: Activity },
  { label: "Event Explorer", href: "/events", icon: Zap, showSSE: true },
  { label: "Audit Log", href: "/audit", icon: ClipboardList },
  { label: "Admin", href: "/admin", icon: Settings },
];

export function Sidebar() {
  const pathname = usePathname();
  const { connected } = useSSE();

  return (
    <aside className="fixed left-0 top-0 h-full w-60 border-r border-border bg-background flex flex-col">
      {/* Branding */}
      <div className="flex items-center gap-2 px-4 py-5 border-b border-border">
        <ListTree className="size-5 text-primary shrink-0" />
        <span className="font-semibold text-sm tracking-tight">StreamRune</span>
        <span className="text-xs text-muted-foreground ml-auto">Demo</span>
      </div>

      {/* Nav */}
      <nav className="flex-1 overflow-y-auto py-3 px-2 space-y-0.5">
        {NAV_ITEMS.map(({ label, href, icon: Icon, showSSE }) => {
          const isActive =
            href === "/" ? pathname === "/" : pathname.startsWith(href);
          return (
            <Link
              key={href}
              href={href}
              className={cn(
                "flex items-center gap-2.5 rounded-md px-3 py-2 text-sm transition-colors",
                isActive
                  ? "bg-accent text-accent-foreground font-medium"
                  : "text-muted-foreground hover:bg-accent/50 hover:text-foreground"
              )}
            >
              <Icon className="size-4 shrink-0" />
              <span className="flex-1">{label}</span>
              {showSSE && (
                <span
                  className={cn(
                    "size-2 rounded-full shrink-0",
                    connected ? "bg-green-500" : "bg-muted-foreground"
                  )}
                  title={connected ? "SSE connected" : "SSE disconnected"}
                />
              )}
            </Link>
          );
        })}
      </nav>

      {/* Role switcher */}
      <div className="border-t border-border p-3">
        <RoleSwitcher />
      </div>
    </aside>
  );
}
