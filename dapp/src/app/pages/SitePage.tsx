import React from "react";
import { NavLink } from "react-router-dom";
import ReactMarkdown from "react-markdown";
import remarkBreaks from "remark-breaks";
import {
  getWalletAddress,
  getDataFromAO,
  connectWallet,
  messageToAO,
  shortAddr,
} from "../util/util";
import { AO_PET, AO_RELIFE } from "../util/consts";
import { Server } from "../../server/server";
import Portrait from "../elements/Portrait";
import { subscribe } from "../util/event";
import "./SitePage.css";

import { BsWallet2 } from "react-icons/bs";

import PetCard from "../elements/PetCard"; // Import the PetCard component

import NavBar from "../elements/NavBar";
import {
  Card,
  CardContent,
  Box,
  Typography,
  Button,
  Container,
} from "@mui/material"; // Add this import

// Add this new import for the countdown timer
import Countdown, { CountdownRenderProps } from "react-countdown";

// Add this import for the Twitter SVG icon
import { FaTwitter } from "react-icons/fa";

interface Pet {
  name: string;
  description: string;
  level: number;
  type: number;
  id: number;
  lastUpdated: number;
}

interface SitePageState {
  users: number;
  posts: number;
  replies: number;
  open: boolean;
  address: string;
  openMenu: boolean;
  count: number;
  message?: string;
  name: string;
  description: string;
  pet: Pet | null; // Allow pet to be null
  showMessageBox: boolean; // New state variable for message box
  generateStatus: string; // New state variable for generation status
  generateStatusHistory: string[]; // New state variable for status history
}

class SitePage extends React.Component<{}, SitePageState> {
  constructor(props: {}) {
    super(props);
    const address = Server.service.isLoggedIn();
    this.state = {
      name: "",
      description: "",
      users: 0,
      posts: 0,
      replies: 0,
      open: false,
      address,
      openMenu: false,
      count: 0,
      message: "",
      pet: null, // Initialize pet as null
      showMessageBox: false, // Initialize showMessageBox as false
      generateStatus: "", // Initialize generateStatus as an empty string
      generateStatusHistory: [], // Initialize as an empty array
    };

    subscribe("wallet-events", () => {
      let address = Server.service.isLoggedIn();
      this.setState({ address });
    });
    this.handleNameChange = this.handleNameChange.bind(this);
    this.handleDescriptionChange = this.handleDescriptionChange.bind(this);
    this.handleClick = this.handleClick.bind(this);
    this.handleFeed = this.handleFeed.bind(this); // Bind handleFeed
    this.closeMessageBox = this.closeMessageBox.bind(this); // Bind closeMessageBox
    this.handleGenerateOnChainLife = this.handleGenerateOnChainLife.bind(this); // Bind the new method
  }

  handleNameChange(event: { target: { value: any } }) {
    this.setState({ name: event.target.value });
  }

  handleDescriptionChange(event: { target: { value: any } }) {
    this.setState({ description: event.target.value });
  }

  componentDidMount() {
    Server.service.checkPermisson();
    this.start();
  }

  async initPet() {
    let response = await messageToAO(
      AO_PET,
      {
        name: this.state.name,
        description: this.state.description,
        address: this.state.address,
      },
      "initPet"
    );
    console.log("response:", response);
    this.getPet(this.state.address);
  }

  async updateLevel() {
    let response = await messageToAO(
      AO_PET,
      { address: this.state.address },
      "updateLevel"
    );
    console.log("response:", response);
    this.getPet(this.state.address); // Refresh pet data after feeding
  }

  async getPet(address: string) {
    console.log("address which is getting pet:", address);
    try {
      let replies = await getDataFromAO(AO_PET, "getPet", { address: address });
      console.log("getPet:", replies);
      if (replies && replies.length > 0) {
        this.setState({ pet: replies[0] });
      } else {
        this.setState({ pet: null });
      }
    } catch (error) {
      console.error("Error fetching pet data:", error);
      this.setState({ pet: null });
    }
  }

  async checkNameUnique(name: string) {
    let replies = await getDataFromAO(AO_PET, "checkNameUnique", {
      name: name,
    });
    console.log("checkName:", replies);
    return replies;
  }

  async getCount() {
    let replies = await getDataFromAO(AO_RELIFE, "getCount");
    console.log("get count:", replies);
    this.setState({ count: replies }); // Update state with the count
  }

  async start() {
    this.getCount();
  }

  async disconnectWallet() {
    this.setState({ message: "Disconnect..." });

    Server.service.setIsLoggedIn("");
    Server.service.setActiveAddress("");
    localStorage.removeItem("id_token");

    this.setState({ address: "", message: "" });
  }

  async connect2ArConnect() {
    let connected = await connectWallet();
    if (connected) {
      let address = await getWalletAddress();
      this.setState({ address: address });
      console.log("user address:", address);
      this.afterConnected(address);
    }
  }

  async afterConnected(address: string, othent?: any) {
    Server.service.setIsLoggedIn(address);
    Server.service.setActiveAddress(address);
    this.getPet(address);
  }

  async handleClick(e: { currentTarget: any }) {
    // Check if the name is unique
    const replied = await this.checkNameUnique(this.state.name);
    if (replied.unique === false) {
      console.log("Name has been used！名字已经被占用辣！");
      alert("Name has been used！名字已经被占用辣！");
      this.setState({ showMessageBox: true }); // Show message box
    } else {
      console.log("Button clicked!");
      this.initPet();
      setTimeout(() => {
        this.getCount();
        this.getPet(this.state.address);
      }, 1000); // Delay getCount by 1 second
    }
  }

  handleFeed() {
    this.updateLevel();
  }

  closeMessageBox() {
    this.setState({ showMessageBox: false }); // Close message box
  }

  isButtonDisabled() {
    const { name, description, address } = this.state;
    return !name || !description || !address;
  }

  async initAcct() {
    console.log("Initializing account...");
    try {
      let response = await messageToAO(AO_RELIFE, { address: this.state.address }, "initAcct");
      console.log("Account initialized:", response);
    } catch (error) {
      console.error("Error initializing account:", error);
    }
  }

  async initLife(x: number, y: number, z: number) {
    console.log("Initializing life...");
    try {
      let response = await messageToAO(AO_RELIFE, { address: this.state.address, x: x, y: y, z: z }, "initLife");
      console.log("Life initialized:", response);
    } catch (error) {
      console.error("Error initializing life:", error);
    }
  }

  async updateLife() {
    console.log("Updating life...");
    try {
      let response = await messageToAO(AO_RELIFE, { address: this.state.address }, "updateLife");
      console.log("Life updated:", response);
    } catch (error) {
      console.error("Error updating life:", error);
    }
  }

  async getLife() {
    let replies = await getDataFromAO(AO_RELIFE, "getLifes", { address: this.state.address });
    console.log("getLifes:", replies);
    return replies;
  }

  async handleGenerateOnChainLife() {
    console.log("Generating a series of on-chain-life...");
    await this.initAcct();
    await new Promise(resolve => setTimeout(resolve, 1000)); // Delay 1 second

    const { type, level } = this.state.pet || {};
    const x = 5 * ((type || 0) % 100 + (level || 0) % 100);
    const y = 5 * (((type || 0) + 68) % 100 + ((level || 0) + 33) % 100);
    const z = Math.floor(Math.random() * 1001) - 500; // Random between [-500, 500]

    this.setState((prevState) => ({
      generateStatus: "Your birth point is: " + x + ", " + y + ", " + z,
      generateStatusHistory: [...prevState.generateStatusHistory, "Your birth point is: " + x + ", " + y + ", " + z]
    }));
    await this.initLife(x, y, z);
    await new Promise(resolve => setTimeout(resolve, 1000)); // Delay 1 second

    for (let i = 0; i < 10; i++) {
      await this.updateLife();
      await new Promise(resolve => setTimeout(resolve, 2000)); // Delay 2 seconds

      // Fetch the latest life data
      const lifeData = await this.getLife();
      if (lifeData && lifeData.length > 0) {
        console.log("lifeData:", lifeData[lifeData.length - 1]);
        const lifeEventsString = lifeData[lifeData.length - 1].lifeEvents;
        try {
          // Try to parse the events as JSON
          const lifeEvents = JSON.parse(lifeEventsString);
          console.log("events:", lifeEvents);
          
          const latestEvent = lifeEvents[lifeEvents.length - 1];
          const eventNum = parseInt(latestEvent, 16) % 10 + 1; // Convert hex to decimal, mod 10, add 1

          console.log(`Event number for step ${i + 1}:`, eventNum);

          // Construct the URL with the event number
          const eventUrl = `https://ao-relife.deno.dev/event?unique_id=${i+1}&event_num=${eventNum}`;
          console.log(`Event URL for step ${i + 1}:`, eventUrl);

          // Fetch the event data from the URL
          try {
            const response = await fetch(eventUrl);
            const data = await response.json();
            console.log(`Event for step ${i + 1}:`, data.event);
            
            // KEEP THIS LINE! DONT DELETE IT!
            // TODO: make sure why it not working:Split the event string into CN and EN parts
            // const [cnPart, enPart] = data.event.split(/\n/);
            // console.log("cnPart:", cnPart);
            // console.log("enPart:", enPart);
            // Update the state with the split event data

            if (data.event.includes("#DEAD")) {
              this.setState((prevState) => ({
                generateStatus: data.event.replace('#DEAD', '') || '',
                generateStatusHistory: [...prevState.generateStatusHistory, data.event.replace('#DEAD', '') || '']
              }));
              break;
            }
            this.setState((prevState) => ({
              generateStatus: data.event.replace('#DEAD', '') || '',
              generateStatusHistory: [...prevState.generateStatusHistory, data.event.replace('#DEAD', '') || '']
            }));
          } catch (error) {
            console.error(`Error fetching event for step ${i + 1}:`, error);
          }
        } catch (error) {
          console.error("Error parsing life events JSON:", error);
          // Fallback to the original hex parsing if JSON fails
          const lifeEvents = lifeEventsString.match(/.{1,64}/g) || [];
          console.log("events (fallback hex parsing):", lifeEvents);
          
          const latestEvent = lifeEvents[lifeEvents.length - 1];
          const eventNum = parseInt(latestEvent, 16) % 10 + 1; // Convert hex to decimal, mod 10, add 1

          console.log(`Event number for step ${i + 1}:`, eventNum);

          // Construct the URL with the event number
          const eventUrl = `https://ao-relife.deno.dev/event?unique_id=${i+1}&event_num=${eventNum}`;
          console.log(`Event URL for step ${i + 1}:`, eventUrl);

          // Fetch the event data from the URL
          try {
            const response = await fetch(eventUrl);
            const data = await response.json();
            console.log(`Event for step ${i + 1}:`, data.event);
          } catch (error) {
            console.error(`Error fetching event for step ${i + 1}:`, error);
          }
        }
      }
    }

    this.setState((prevState) => ({
      generateStatus: "You have generated a complete on-chain life.",
      generateStatusHistory: [...prevState.generateStatusHistory, "You have generated a complete on-chain life."]
    }));
  }

  render() {
    let shortAddress = shortAddr(this.state.address, 4);
    const codeStyle = {
      lineHeight: "1.2", // Adjust the line height to reduce spacing
      padding: "10px", // Adjust padding as needed
      margin: "0", // Remove default margins
    };

    const aoLinkUrl = `https://www.ao.link/#/entity/${AO_PET}`; // Construct the URL dynamically

    // Calculate the target date in UTC
    const targetDate = new Date("2024-09-14T04:00:00Z"); // 12:00:00 UTC+8 is 04:00:00 UTC

    // Add this function to render the countdown with explicit typing
    const renderer = ({
      days,
      hours,
      minutes,
      seconds,
      completed,
    }: CountdownRenderProps) => {
      if (completed) {
        return <Typography variant="h5">Game is live!</Typography>;
      } else {
        return (
          <Box sx={{ display: "flex", justifyContent: "center", gap: 2 }}>
            {[
              { label: "Days", value: days },
              { label: "Hours", value: hours },
              { label: "Minutes", value: minutes },
              { label: "Seconds", value: seconds },
            ].map(({ label, value }) => (
              <Box key={label} sx={{ textAlign: "center" }}>
                <Box
                  sx={{
                    backgroundColor: "#4a90e2",
                    color: "white",
                    borderRadius: "8px",
                    padding: "10px",
                    minWidth: "60px",
                    fontWeight: "bold",
                    fontSize: "1.5rem",
                  }}
                >
                  {value}
                </Box>
                <Typography variant="caption" sx={{ mt: 1 }}>
                  {label}
                </Typography>
              </Box>
            ))}
          </Box>
        );
      }
    };

    return (
      <div className="app-container">
        <div className="site-page-header-pc">
          <div className="header-container">
            <div className="wallet-container">
              {this.state.address ? (
                <>
                  <div
                    className="app-icon-button connect"
                    onClick={() => this.disconnectWallet()}
                  >
                    {shortAddress}
                  </div>
                  <a href="/#/profile">
                    <div className="profile-button">Profile</div>
                  </a>
                </>
              ) : (
                <div
                  className="app-icon-button connect"
                  onClick={() => this.connect2ArConnect()}
                >
                  <BsWallet2 size={20} />
                  ArConnect
                </div>
              )}
            </div>
            <NavBar address={this.state.address} />
          </div>
          <ReactMarkdown
            remarkPlugins={[remarkBreaks]}
            components={{
              code({ node, className, children, ...props }) {
                return (
                  <code style={codeStyle} {...props}>
                    {children}
                  </code>
                );
              },
              pre({ node, className, children, ...props }) {
                return (
                  <pre style={codeStyle} {...props}>
                    {children}
                  </pre>
                );
              },
              h3({ node, className, children, ...props }) {
                return (
                  <h3
                    style={{
                      color: "#1a237e",
                      textAlign: "center",
                      fontSize: "1.5rem", // Adjust the font size as needed
                    }}
                    {...props}
                  >
                    {children}
                  </h3>
                );
              },
            }}
          >
            ### 人生重开模拟器 Relife
          </ReactMarkdown>
          <br></br>
          <center>
            放置类游戏！基于
            <a
              href="https://ar.dimension-life.rootmud.xyz/"
              target="_blank"
              rel="noopener noreferrer"
              style={{ textDecoration: "underline", color: "blue" }}
            >
              「dimensionLife」
            </a>
            体验你的 AO-on-chain Life!
          </center>
          <center>
            Idle game! Based on
            <a
              href="https://ar.dimension-life.rootmud.xyz/"
              target="_blank"
              rel="noopener noreferrer"
              style={{ textDecoration: "underline", color: "blue" }}
            >
              "dimensionLife"
            </a>
            Try your AO-on-chain Life!
          </center>

          {/* Existing content */}
          <br></br>
          <center>
            <p>
              👇 先来领取你的 dimensionLife，本游戏算法基于 dimensionLife 👇{" "}
            </p>
            <p>
              👇 First, claim your dimensionLife. This game's algorithm is based
              on dimensionLife 👇{" "}
            </p>
          </center>
          <br></br>
          {/* Existing input fields and button */}
          <center>
            <div>
              <label>
                Name:&nbsp;&nbsp;
                <input
                  type="text"
                  value={this.state.name}
                  onChange={this.handleNameChange}
                />
              </label>
            </div>
            <br></br>
            <div>
              <label>
                Description:&nbsp;&nbsp;
                <input
                  type="text"
                  value={this.state.description}
                  onChange={this.handleDescriptionChange}
                />
              </label>
            </div>
          </center>
          <br></br>
          <div className="button-container">
            <button
              onClick={this.handleClick}
              disabled={this.isButtonDisabled()}
              style={{
                backgroundColor: this.isButtonDisabled() ? "#d3d3d3" : "",
                cursor: this.isButtonDisabled() ? "not-allowed" : "pointer",
              }}
            >
              Get My Pet (Free Now!)
            </button>
          </div>
          <br></br>
          {this.state.pet && ( // Conditionally render PetCard if pet is not null
            <PetCard
              id={this.state.pet.id}
              name={this.state.pet.name}
              description={this.state.pet.description}
              level={this.state.pet.level}
              type={this.state.pet.type}
              lastUpdated={this.state.pet.lastUpdated}
              onFeed={this.handleFeed} // Pass handleFeed as prop
            />
          )}
          
          <center>
            <p>Onchain life totally:</p>
            <p>
              <b>&lt;{this.state.count}&gt;</b>
            </p>
          </center>
          <br></br>
          {this.state.pet && (
            <div className="button-container">
              <button
                onClick={this.handleGenerateOnChainLife}
                style={{
                  backgroundColor: "#4a90e2",
                  color: "white",
                  cursor: "pointer",
                  width: "300px", // Set the desired width
                }}
              >
                Generate my On-Chain Life...
              </button>
            </div>
          )}
          <div style={{ textAlign: "center" }}>
            {this.state.generateStatus && ( // Conditionally render the status message
              <div>
                <p>{this.state.generateStatus}</p>
              </div>
            )}
          </div>
          <br></br>
          {this.state.generateStatusHistory.length > 0 && (
            <div className="life-card-container" style={{ textAlign: "center" }}>
              <h3>Life Generation History</h3>
              {this.state.generateStatusHistory.map((status, index) => (
                <div key={index} className="life-card">
                  {status.split('\n').map((line, lineIndex) => (
                    <p key={lineIndex}>{line}</p>
                  ))}
                </div>
              ))}
            </div>
          )}
          <br></br>
          <div className="button-container">
            <a
              href="https://x.com/0xleeduckgo"
              target="_blank"
              rel="noreferrer"
            >
              <button className="white-button">view the Author Twitter</button>
            </a>
            <br></br>
            <a href="https://t.me/rootmud" target="_blank" rel="noreferrer">
              <button className="white-button">telegram group</button>
            </a>
            <br></br>
            <a
              href={aoLinkUrl} // Use the dynamic URL here
              target="_blank"
              rel="noreferrer"
            >
              <button className="white-button">
                check the description on ao.link
              </button>
            </a>
          </div>
          <br></br>
          {/* {this.state.showMessageBox && ( // Conditionally render message box
            <div className="message-box">
              <p>Name has been used! Please choose another name.</p>
              <button onClick={this.closeMessageBox}>Close</button>
            </div>
          )} */}

        </div>

        {/* FOR MOBILE */}
        <div className="site-page-header-mobile">
          <Portrait />
          <p>mobile version is not supportted yet.</p>
        </div>
      </div>
    );
  }
}

export default SitePage;
