import type { ReactNode } from "react";
import { Sidebar } from "./sidebar";

export function Shell({ children }: { children: ReactNode }) {
  return (
    <div className="flex h-full min-h-screen">
      <Sidebar />
      <main className="ml-60 flex-1 overflow-auto p-6">{children}</main>
    </div>
  );
}
