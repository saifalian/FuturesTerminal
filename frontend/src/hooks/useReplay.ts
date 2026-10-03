import { useEffect, useState } from "react";

export function useReplayFiles() {
  const [files, setFiles] = useState<string[]>([]);

  useEffect(() => {
    fetch("http://127.0.0.1:8000/replay/files")
      .then((r) => r.json())
      .then((d) => setFiles(d.files ?? []))
      .catch(() => setFiles([]));
  }, []);

  return files;
}
