// Vite/React entry point.
// Mounts the <App /> component tree into <div id="root"> and pulls in the
// global Tailwind stylesheet. StrictMode is enabled so dev-time bugs (double
// effect runs, deprecated lifecycle calls, etc.) surface early. The trailing
// `!` is the standard non-null assertion — index.html guarantees the root
// element exists.

import React from "react";
import ReactDOM from "react-dom/client";
import App from "./App";
import "./index.css";

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>
);
