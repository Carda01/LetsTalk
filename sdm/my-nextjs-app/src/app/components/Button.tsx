import { RefreshCcw } from "lucide-react"; // make sure lucide-react is installed
import React from "react";

interface RefreshButtonProps {
  onClick: () => void;
  loading?: boolean;
}

const RefreshButton: React.FC<RefreshButtonProps> = ({ onClick, loading = false }) => {
  return (
    <button
      onClick={onClick}
      className="flex h-10 items-center gap-2 px-4 bg-blue-600 text-white rounded-xl hover:bg-blue-700 transition disabled:opacity-50"
      disabled={loading}
    >
      <RefreshCcw
        className={`w-5 h-5 ${loading ? "animate-spin" : ""}`}
      />
      {loading ? "Refreshing..." : "Refresh"}
    </button>
  );
};

export default RefreshButton;
