const defaultTz = "America/Los_Angeles";

const getDefaultTimezone = () => {
  return moment.tz.guess() || defaultTz;
};

export const convertTimestampWithTz = (timestamp, format = "YYYY/MM/DD") =>
  moment.tz(timestamp, getDefaultTimezone()).format(format);
