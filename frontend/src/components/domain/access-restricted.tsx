import { ShieldOff } from "lucide-react";

/**
 * What a page shows instead of its content when the current role may not read
 * it. The page sends no request in that state: the backend would answer 403.
 */
export function AccessRestricted({ message }: { message: string }) {
  return (
    <div className="flex flex-col items-center justify-center py-24 gap-4 text-center">
      <ShieldOff className="size-12 text-muted-foreground opacity-40" />
      <div>
        <h2 className="text-lg font-semibold">Access Restricted</h2>
        <p className="text-sm text-muted-foreground mt-1">{message}</p>
      </div>
    </div>
  );
}
